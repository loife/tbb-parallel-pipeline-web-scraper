#include <vector>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <atomic>
#include <sstream>
#include "tbb/tbb.h"
#include "cpr/cpr.h"
#include "gumbo.h"

int NUM_TOKENS = 15;

std::atomic<int> visited_pages_num{ 0 };
std::atomic<int> total_requests{ 0 };
std::atomic<int> successful_requests{ 0 };
std::atomic<int> failed_requests{ 0 };

oneapi::tbb::concurrent_vector<int> ratings;
oneapi::tbb::concurrent_vector<int> prices;
std::atomic<int> five_star_number{ 0 };
std::atomic<int> one_star_number{ 0 };
std::atomic<int> number_of_expensive{ 0 };
std::atomic<int> number_of_cheap{ 0 };

int max_price = 0;
std::string max_price_title;

oneapi::tbb::mutex max_price_mutex;
oneapi::tbb::mutex print_mutex;


class URLReader {
private:
    mutable std::ifstream in;
    std::string fname;
public:

    URLReader(const std::string& filename) : in(filename), fname(filename) {
        if (!in.is_open()) {
            throw std::runtime_error("Error opening file.");
        }
    };

    URLReader(const URLReader& f) : in(f.fname), fname(f.fname) {
        if (!in.is_open()) {
            throw std::runtime_error("Error opening file.");
        }
    };

    ~URLReader() {};

    std::string operator()(oneapi::tbb::flow_control& fc) const {
        std::string line;

        if (std::getline(in, line)) {
            return std::string(std::move(line));
        }
        fc.stop();
        return std::string();
    }
};

class RequestSender {
    int max_retries;
    int base_backoff_ms;
    int max_backoff_ms;
    int connect_timeout_ms;
    int total_timeout_ms;

public:
    RequestSender(int max_retries = 5,
        int base_backoff_ms = 200,
        int max_backoff_ms = 1000,
        int connect_timeout_ms = 2000,
        int total_timeout_ms = 10000)
        : max_retries(max_retries),
        base_backoff_ms(base_backoff_ms),
        max_backoff_ms(max_backoff_ms),
        connect_timeout_ms(connect_timeout_ms),
        total_timeout_ms(total_timeout_ms)
    {}

    std::string operator()(std::string input) const {
        if (input.empty()) return std::string();

        cpr::Session session;
        session.SetUrl(cpr::Url{ input });
        session.SetConnectTimeout(cpr::ConnectTimeout(connect_timeout_ms));
        session.SetTimeout(cpr::Timeout(total_timeout_ms));
        
       visited_pages_num++;

        for (int attempt = 0; attempt <= max_retries; ++attempt) {

            cpr::Response response = session.Get();
            total_requests++;

            if (response.error.code == cpr::ErrorCode::OK) { // Da li je došlo do problema u transferu?
                double status = response.status_code;

                if (status >= 200 && status < 300 && !response.text.empty()) { // Sve u redu
                    successful_requests++;
                    return response.text;
                }

                failed_requests++;

                if (status == 429) { // Too many requests
                    int retry_after = std::stoi(response.header["Retry-After"]);
                    std::this_thread::sleep_for(std::chrono::seconds(retry_after));
                    continue;
                }

                if (status >= 400 && status < 500) {
                    {
                        oneapi::tbb::mutex::scoped_lock lock(print_mutex);
                        std::cout << "Client error " << status << " on URL " << input << ". Not retrying." << std::endl;
                    }
                    return std::string();
                }

                if (status > 500) { // Greška na serveru, pokušavamo opet
                    std::random_device rd;
                    std::mt19937 gen(rd());

                    std::uniform_int_distribution<> distrib(base_backoff_ms, max_backoff_ms);
                    int wait_ms = distrib(gen);
                    std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms)); // Čekaj pre retry
                }
            }
        }
        {
            oneapi::tbb::mutex::scoped_lock lock(print_mutex);
            std::cout << "Too many retries on URL \"" << input << "\"." << std::endl;
        }

        return std::string();
    }
};

class HttpAnalyzer {
    void analyze_html(GumboNode* root) const {
        if (!root) return;
        if (root->type == GUMBO_NODE_ELEMENT) {
            scan_article(root);
        }
        else if (root->type == GUMBO_NODE_DOCUMENT) {
            GumboVector* children = &root->v.document.children;
            for (unsigned int i = 0; i < children->length; ++i) {
                GumboNode* child = static_cast<GumboNode*>(children->data[i]);
                if (child->type == GUMBO_NODE_ELEMENT) scan_article(child);
            }
        }
    }

    void scan_article(GumboNode* node) const {
        if (node->type != GUMBO_NODE_ELEMENT) return;

        // If this element is an article or has product_pod class, treat as product node
        bool is_product_pod = (node->v.element.tag == GUMBO_TAG_ARTICLE) ||
            has_class(node, "product_pod");

        if (is_product_pod) {
            GumboVector* children = &node->v.element.children;
            for (unsigned int i = 0; i < children->length; ++i) {
                GumboNode* child = static_cast<GumboNode*>(children->data[i]);
                if (child->type == GUMBO_NODE_ELEMENT) {

                    // Read ratings
                    if (child->v.element.tag == GUMBO_TAG_P) {
                        GumboAttribute* classAttr = gumbo_get_attribute(&child->v.element.attributes, "class");
                        if (classAttr) {
                            std::string cls(classAttr->value);

                            int rating = get_number_rating(cls);
                            if (rating == 5) five_star_number++;
                            if (rating == 1) one_star_number++;

                            ratings.push_back(rating);
                        }
                    }

                    // Read prices
                    if (child->v.element.tag == GUMBO_TAG_DIV) {
                        if (has_class(child, "product_price")) {
                            GumboVector* price_children = &child->v.element.children;
                            for (unsigned int i = 0; i < price_children->length; ++i) {
                                GumboNode* price_node = static_cast<GumboNode*>(price_children->data[i]);
                                if (has_class(price_node, "price_color")) {
                                    std::string raw_text = get_text(price_node);

                                    raw_text.erase(
                                        std::remove_if(raw_text.begin(), raw_text.end(),
                                            [](unsigned char c) { return !std::isdigit(c) && c != '.'; }),
                                        raw_text.end());

                                    double price = 0.0;
                                    if (!raw_text.empty()) {
                                        try {
                                            price = std::stod(raw_text);
                                        }
                                        catch (const std::exception& e) {
                                            //std::cout << "Error converting price" << raw_text << std::endl;
                                            continue; // Ovo ce preskociti loop, proveri kasnije
                                        }
                                    }

                                    prices.push_back(price);

                                    if (price > 50) {
                                        number_of_expensive++;
                                    }

                                    if (price < 20) {
                                        number_of_cheap++;
                                    }

                                    {
                                        oneapi::tbb::mutex::scoped_lock lock(max_price_mutex);
                                        if (price > max_price) {
                                            max_price = price;
                                            max_price_title = extract_title_from_article(node);
                                        }
                                    } // mutex bi trebalo da je unisten ovde
                                }
                            }
                        }
                    }
                }
            }
            return;
        }
        // Recurse children
        GumboVector* children = &node->v.element.children;
        for (unsigned int i = 0; i < children->length; ++i) {
            GumboNode* child = static_cast<GumboNode*>(children->data[i]);
            scan_article(child);
        }
    }

    bool has_class(GumboNode* node, const std::string& cls) const {
        if (node->type != GUMBO_NODE_ELEMENT) return false;
        GumboAttribute* attr = gumbo_get_attribute(&node->v.element.attributes, "class");
        if (!attr) return false;
        std::string val(attr->value);
        
        return val.find(cls) != std::string::npos;
    }

    std::string extract_title_from_article(GumboNode* article_node) const {
        if (!article_node || article_node->type != GUMBO_NODE_ELEMENT) return {};

        GumboVector* children = &article_node->v.element.children;
        for (unsigned int i = 0; i < children->length; ++i) {
            GumboNode* child = static_cast<GumboNode*>(children->data[i]);
            if (child->type == GUMBO_NODE_ELEMENT && child->v.element.tag == GUMBO_TAG_H3) {

                // Trazimo <a> u <h3>
                GumboVector* h3_children = &child->v.element.children;
                for (unsigned int j = 0; j < h3_children->length; ++j) {
                    GumboNode* a = static_cast<GumboNode*>(h3_children->data[j]);
                    if (a->type == GUMBO_NODE_ELEMENT && a->v.element.tag == GUMBO_TAG_A) {
                        GumboAttribute* title_attr = gumbo_get_attribute(&a->v.element.attributes, "title");
                        if (title_attr) return std::string(title_attr->value);
                        
                        // fallback
                        return get_text(a);
                    }
                }
            }
        }
        return {};
    }

    std::string get_text(GumboNode* node) const {
        if (!node) return {};
        if (node->type == GUMBO_NODE_TEXT) {
            return std::string(node->v.text.text);
        }
        else if (node->type == GUMBO_NODE_ELEMENT || node->type == GUMBO_NODE_TEMPLATE) {
            std::string out;
            GumboVector* children = &node->v.element.children;
            for (unsigned int i = 0; i < children->length; ++i) {
                GumboNode* child = static_cast<GumboNode*>(children->data[i]);
                out += get_text(child);
            }
            return out;
        }
        return {};
    }

    int get_number_rating(const std::string& cls) const {
        std::istringstream iss(cls);
        std::string token;
        while (iss >> token) {
            if (token == "star-rating") continue;

            std::transform(token.begin(), token.end(), token.begin(),
                [](unsigned char c) { return std::tolower(c); });

            if (token == "one")   return 1;
            if (token == "two")   return 2;
            if (token == "three") return 3;
            if (token == "four")  return 4;
            if (token == "five")  return 5;

        }
        return 0;
    }

public:
    HttpAnalyzer(){}

    void operator()(std::string input) const {
        if (input=="") return;

        GumboOutput* output = gumbo_parse(input.c_str());
        if (!output) return;

        analyze_html(output->root);

        gumbo_destroy_output(&kGumboDefaultOptions, output);
    }
};


int main() {
    oneapi::tbb::parallel_pipeline(NUM_TOKENS,
        oneapi::tbb::make_filter<void, std::string>(
            oneapi::tbb::filter_mode::serial_in_order, URLReader("input_file.txt")) &
        oneapi::tbb::make_filter<std::string, std::string>(
            oneapi::tbb::filter_mode::parallel, RequestSender()) &
        oneapi::tbb::make_filter<std::string, void>(
            oneapi::tbb::filter_mode::parallel, HttpAnalyzer())
    );

    using namespace std;

    const size_t n_prices = prices.size();
    const size_t n_ratings = ratings.size();

    double sum_prices = 0;
    for (size_t i = 0; i < n_prices; ++i) sum_prices += prices[i];

    long long sum_ratings = 0;
    for (size_t i = 0; i < n_ratings; ++i) sum_ratings += ratings[i];

    double avg_price = n_prices ? static_cast<double>(sum_prices) / static_cast<double>(n_prices) : 0.0;
    double avg_rating = n_ratings ? static_cast<double>(sum_ratings) / static_cast<double>(n_ratings) : 0.0;


    cout << "\n================== Request summary ==================\n";

    cout << left << setw(36) << "Number of pages visited" << ": " << visited_pages_num.load() << '\n';

    cout << left << setw(36) << "Total requests" << ": " << total_requests.load() << '\n';
    cout << left << setw(36) << "Failed requests" << ": " << failed_requests.load() << '\n';
    cout << left << setw(36) << "Successful requests" << ": " << successful_requests.load() << '\n';


    cout << "\n================== Books summary ==================\n";

    cout << left << setw(36) << "Number of books scanned" << ": " << n_prices << '\n';

    cout << fixed << setprecision(2);
    cout << left << setw(36) << "Average price" << ": " << avg_price << '\n';
    cout << left << setw(36) << "Average rating" << ": " << avg_rating << " / 5" << '\n';

    cout << left << setw(36) << "Number of 5-star books" << ": " << five_star_number.load() << '\n';
    cout << left << setw(36) << "Number of 1-star books" << ": " << one_star_number.load() << '\n';
    cout << left << setw(36) << "Number of books with price > 50" << ": " << number_of_expensive.load() << '\n';
    cout << left << setw(36) << "Number of books with price < 20" << ": " << number_of_cheap.load() << '\n';


    cout << left << setw(36) << "Most expensive book" << ": \"" << max_price_title << "\"\n";
    cout << left << setw(36) << "Most expensive book price" <<  ": " << max_price << '\n';



    cout << "===================================================\n\n";

}
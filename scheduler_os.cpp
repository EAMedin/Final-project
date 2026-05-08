// Final Project for CS-4352



#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic> // ?
#include <chrono>
#include <condition_variable> //This
#include <cctype>
#include <csignal>
#include <cstdlib>
#include <deque>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex> // ?
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>


using namespace std;

struct HttpReply {
    int code = 0;
    string body;
};

struct Elevator {
    string name;
    int low = 0;
    int high = 0;
    int start_floor = 0;
    int capacity = 0;
};

struct Status {
    int floor = 0;
    char dir = 'S';
    int riders = 0;
    int open_slots = 0;
    bool has_fresh_data = false;
};

struct Person {
    string name;
    int from_floor = 0;
    int to_floor = 0;
    int retries = 0;
};
/*
 *  just to make sure the code is somewhat readable
 */
string trim(const string& raw_text) {
    size_t left = 0;
    while (left < raw_text.size() && isspace(static_cast<unsigned char>(raw_text[left]))) {
        left++;
    }

    size_t right = raw_text.size();
    while (right > left && isspace(static_cast<unsigned char>(raw_text[right - 1]))) {
        right--;
    }

    return raw_text.substr(left, right - left);
}

std::vector<string> split(const string& line_text, char divider) {
    std::vector<string> pieces;
    string part_text;
    stringstream line_reader(line_text);

    while (getline(line_reader, part_text, divider)) {
        pieces.push_back(part_text);
    }

    return pieces;
}

/*
opens a fresh socket to the port.
*/
int connect_loopback(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw runtime_error("Could not create socket");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) <= 0) {
        close(fd);
        throw runtime_error("Could not read loopback address");
    }

    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        throw runtime_error("Could not connect to local API");
    }

    return fd;
}
/*
send the whole buffer. 
*/
void send_all(int fd, const string& data) {
    size_t sent = 0;

    while (sent < data.size()) {
        ssize_t n = send(
            fd,
            data.data() + sent,
            data.size() - sent,
            0
        );

        if (n < 0) {
            throw runtime_error("Could not send request");
        }

        sent += static_cast<size_t>(n);
    }
}
/*
reads the whole response. make sure we are done.
*/
string recv_all(int fd) {
    string out;
    char buf[4096];

    while (true) {
        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n < 0) {
            throw runtime_error("Could not receive response");
        }
        if (n == 0) {
            break;
        }
        out.append(buf, static_cast<size_t>(n));
    }

    return out;
}
/*
parse a raw HTTP response. into a statuscode and body. 
*/
HttpReply parse_resp(const string& raw) {
    size_t split_at = raw.find("\r\n\r\n");
    if (split_at == string::npos) {
        throw runtime_error("Bad HTTP reply");
    }

    string head = raw.substr(0, split_at);
    string body = raw.substr(split_at + 4);

    istringstream head_ss(head);
    string status_line;
    getline(head_ss, status_line);
    if (!status_line.empty() && status_line.back() == '\r') {
        status_line.pop_back();
    }

    istringstream sl(status_line);
    string http_ver;
    HttpReply r;
    sl >> http_ver >> r.code;
    r.body = trim(body);
    return r;
}
/*
the HTTP. 
*/
HttpReply http(const string& method, int port, const string& path) {
    int fd = connect_loopback(port);

    ostringstream req;
    req << method << " " << path << " HTTP/1.1\r\n";
    req << "Host: 127.0.0.1:" << port << "\r\n";
    req << "Connection: close\r\n";
    req << "\r\n";

    send_all(fd, req.str());
    string raw = recv_all(fd);
    close(fd);

    return parse_resp(raw);
}

/*
 the thread-safe fifo with condition variable so consumer can wait without endless busy loop.
 */
class RideQueue {
public:
    void push(const Person& p) {
        lock_guard<mutex> lk(m);
        q.push_back(p);
        cv.notify_one();
    }

    bool pop(Person& out, chrono::milliseconds wait) {
        unique_lock<mutex> lk(m);
        cv.wait_for(lk, wait, [this]() {
            return closed || !q.empty();
        });

        if (q.empty()) {
            return false;
        }

        out = q.front();
        q.pop_front();
        return true;
    }

    void close_q() {
        lock_guard<mutex> lk(m);
        closed = true;
        cv.notify_all();
    }

    bool empty() {
        lock_guard<mutex> lk(m);
        return q.empty();
    }

private:
    deque<Person> q;
    mutex m;
    condition_variable cv;
    bool closed = false;
};

/*
    set up scheduler. reads building file at startup. 
*/
class Scheduler {
public:
    Scheduler(string bldg_path, int api_port):
    path(std::move(bldg_path)), port(api_port) {
        load_building();
    }

    int run() {
        cout << "Scheduler is preparing the elevator simulation." << endl;

        HttpReply r = http("PUT", port, "/Simulation/start");
        if (r.code != 200 && r.code != 202) {
            cerr << "Could not start simulation: " << r.body << endl;
            return 1;
        }

        running.store(true);

        thread status_th(&Scheduler::status_worker, this);
        thread input_th(&Scheduler::input_worker, this);
        thread dispatch_th(&Scheduler::dispatch_worker, this);

        input_th.join();
        dispatch_th.join();
        status_th.join();

        cout << "Scheduler finished cleanly." << endl;
        return 0;
    }

private:
    string path;
    int port = 0;

    std::vector<Elevator> elevators;
    map<string, Status> status;
    map<string, int> pending;

    mutex state_lock;
    RideQueue queue;

    atomic<bool> running{false};
    atomic<bool> sim_done{false};
    atomic<bool> input_done{false};

    //parses the building file into elevator table
    void load_building() {
        ifstream f(path);
        if (!f.is_open()) {
            throw runtime_error("Could not open building file");
        }

        string line;
        while (getline(f, line)) {
            line = trim(line);
            if (line.empty()) {
                continue;
            }

            std::vector<string> cols = split(line, '\t');
            if (cols.size() != 5) {
                throw runtime_error("Bad building file format");
            }

            Elevator e;
            e.name = trim(cols[0]);
            e.low = stoi(trim(cols[1]));
            e.high = stoi(trim(cols[2]));
            e.start_floor = stoi(trim(cols[3]));
            e.capacity = stoi(trim(cols[4]));
            elevators.push_back(e);

            Status s;
            s.floor = e.start_floor;
            s.dir = 'S';
            s.riders = 0;
            s.open_slots = e.capacity;
            s.has_fresh_data = false;

            status[e.name] = s;
            pending[e.name] = 0;
        }
    }

    bool covers(const Elevator& e, const Person& p) const {
        return e.low <= p.from_floor &&
               p.from_floor <= e.high &&
               e.low <= p.to_floor &&
               p.to_floor <= e.high;
    }


    bool any_covers(const Person& p) const {
        for (const Elevator& e : elevators) {
            if (covers(e, p)) {
                return true;
            }
        }
        return false;
    }

    int dir_of(const Person& p) const {
        if (p.to_floor > p.from_floor) {
            return 1;
        }
        if (p.to_floor < p.from_floor) {
            return -1;
        }
        return 0;
    }

    /*
    rate an elevator vs a person. picks best one to use.
    */
    int score(const Elevator& e, const Status& s, int pend, const Person& p) const {
        int want = dir_of(p);
        int gap = abs(s.floor - p.from_floor);
        int busy = s.riders * 3 + pend * 4;
        int wrong_dir = 0;

        if (s.dir == 'U') {
            if (!(want >= 0 && s.floor <= p.from_floor)) {
                wrong_dir += 6;
            }
        } else if (s.dir == 'D') {
            if (!(want <= 0 && s.floor >= p.from_floor)) {
                wrong_dir += 6;
            }
        }

        int here = 0;
        if (s.floor == p.from_floor) {
            here -= 3;
        }

        int span = (e.high - e.low) / 10;
        return gap * 2 + busy + wrong_dir + span + here;
    }

    string pick_elevator(const Person& p) {
        lock_guard<mutex> lk(state_lock);

        string best;
        int best_score = 0;
        bool found = false;

        string fb_name;
        int fb_load = 0;
        bool fb_found = false;

        for (const Elevator& e : elevators) {
            if (!covers(e, p)) {
                continue;
            }

            const Status& s = status[e.name];

            if (s.dir == 'E') {
                continue;
            }

            int load = s.riders + pending[e.name];

            if (!fb_found || load < fb_load ||
                (load == fb_load && e.name < fb_name)) {
                fb_load = load;
                fb_name = e.name;
                fb_found = true;
            }

/*
 full elevator - re-queu cycle. 
*/
            if (load >= e.capacity) {
                continue;
            }

            int sc = score(
                e,
                s,
                pending[e.name],
                p
            );

            if (!found || sc < best_score ||
                (sc == best_score && e.name < best)) {
                best = e.name;
                best_score = sc;
                found = true;
            }
        }

/*
every elevator was full, fall back to least-loaded.
*/
        if (best.empty() && fb_found) {
            best = fb_name;
        }

        return best;
    }

    void bump_pending(const string& name, int step) {
        lock_guard<mutex> lk(state_lock);
        pending[name] += step;
        if (pending[name] < 0) {
            pending[name] = 0;
        }

/*
cap pending at the elevator capacity, if not re-que people get recounted forever and elevator looks permanent. 

*/
        for (const Elevator& e : elevators) {
            if (e.name == name) {
                if (pending[name] > e.capacity) {
                    pending[name] = e.capacity;
                }
                break;
            }
        }
    }

    void refresh_one(const Elevator& e) {
        HttpReply r = http("GET", port, "/ElevatorStatus/" + e.name);
        if (r.code != 200) {
            return;
        }

        std::vector<string> cols = split(r.body, '|');
        if (cols.size() != 5) {
            return;
        }

        Status fresh;
        fresh.floor = stoi(trim(cols[1]));
        string d = trim(cols[2]);
        fresh.dir = d.empty() ? 'S' : d[0];
        fresh.riders = stoi(trim(cols[3]));
        fresh.open_slots = stoi(trim(cols[4]));
        fresh.has_fresh_data = true;

        lock_guard<mutex> lk(state_lock);

/*
rider count went up then pending people just boarded, drop them off pending then.
*/
        int old_riders = status[e.name].riders;
        int boarded = fresh.riders - old_riders;
        if (boarded > 0) {
            pending[e.name] -= boarded;
            if (pending[e.name] < 0) {
                pending[e.name] = 0;
            }
        }

        status[e.name] = fresh;
    }

/*
keep elevator state fresh and watches for sim end. 
*/
    void status_worker() {
        while (running.load()) {
            try {
                HttpReply r = http("GET", port, "/Simulation/check");
                string body = trim(r.body);

                bool done = body.find("complete") != string::npos
                         || body.find("stopped")  != string::npos;
                bool live = body.find("not running") == string::npos
                         && body.find("running")     != string::npos;

                if (done) {
                    sim_done.store(true);
                }

                if (live) {
                    for (const Elevator& e : elevators) {
                        refresh_one(e);
                    }
                }
            } catch (const exception&) {
                if (sim_done.load()) {
                    break;
                }
            }

            if (sim_done.load() && input_done.load() && queue.empty()) {
                running.store(false);
                queue.close_q();
                break;
            }

            this_thread::sleep_for(chrono::milliseconds(250));
        }
    }


    void input_worker() {
        while (true) {
            try {
                if (sim_done.load()) {
                    break;
                }

                HttpReply r = http("GET", port, "/NextInput");
                if (r.code == 200) {
                    string body = trim(r.body);

                    if (body == "NONE") {

                        this_thread::sleep_for(chrono::milliseconds(50));
                    } else {
                        std::vector<string> cols = split(body, '|');
                        if (cols.size() == 3) {
                            Person p;
                            p.name = trim(cols[0]);
                            p.from_floor = stoi(trim(cols[1]));
                            p.to_floor = stoi(trim(cols[2]));
                            queue.push(p);
                        }
                    }
                } else {
                    this_thread::sleep_for(chrono::milliseconds(150));
                }
            } catch (const exception&) {
                this_thread::sleep_for(chrono::milliseconds(200));
            }
        }

        input_done.store(true);
        queue.close_q();
    }

    bool try_assign(Person& p) {
        string bay = pick_elevator(p);
        if (bay.empty()) {
            return false;
        }

        HttpReply r = http(
            "PUT",
            port,
            "/AddPersonToElevator/" + p.name + "/" + bay
        );

        if (r.code == 200) {
            bump_pending(bay, 1);
            cout << "Assigned " << p.name << " to " << bay
                 << " from floor " << p.from_floor
                 << " to floor " << p.to_floor << endl;
            return true;
        }

        string body = trim(r.body);
        if (body.find("already assigned") != string::npos) {
            return true;
        }

        if (body.find("does not exist") != string::npos) {
            cerr << "Skipped rider " << p.name << ": " << body << endl;
            return true;
        }

        return false;
    }

/*
dispatch thread, pops people off, assigns them to elevators. 
*/
    void dispatch_worker() {
        while (true) {
            if (sim_done.load() && input_done.load() && queue.empty()) {
                running.store(false);
                break;
            }

            Person p;
            if (!queue.pop(p, chrono::milliseconds(250))) {
                continue;
            }


            if (!any_covers(p)) {
                cerr << "Dropped rider " << p.name
                     << ": no elevator covers floors "
                     << p.from_floor << " -> " << p.to_floor << endl;
                continue;
            }

            try {
                bool placed = try_assign(p);
                if (!placed) {
                    p.retries++;
                    if (p.retries < 8) {
                        this_thread::sleep_for(chrono::milliseconds(120));
                        queue.push(p);
                    } else {
                        cerr << "Dropped rider after several retries: "
                             << p.name << endl;
                    }
                }
            } catch (const exception&) {
                p.retries++;
                if (p.retries < 8) {
                    this_thread::sleep_for(chrono::milliseconds(120));
                    queue.push(p);
                }
            }
        }
    }
};

int parse_port(const string& port_text) {
    int p = stoi(port_text);
    if (p < 1024 || p > 65535) {
        throw runtime_error("Port must be between 1024 and 65535");
    }
    return p;
}

int main(int argc, char* argv[]) {

    signal(SIGPIPE, SIG_IGN);

    if (argc < 3) {
        cout << "Usage: scheduler_os <path_to_building_file> <port_number>" << endl;
        return 1;
    }

    try {
        Scheduler s(argv[1], parse_port(argv[2]));
        return s.run();
    } catch (const exception& ex) {
        cout << "Scheduler error: " << ex.what() << endl;
        return 1;
    }
}

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <deque>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std;

struct HttpReply {
    int code = 0;
    string body_text;
};

struct LiftPlan {
    string bay_name;
    int floor_low = 0;
    int floor_high = 0;
    int launch_floor = 0;
    int rider_limit = 0;
};

struct LiftPulse {
    int current_floor = 0;
    char travel_mark = 'S';
    int rider_count = 0;
    int open_slots = 0;
    bool has_fresh_data = false;
};

struct RideCall {
    string rider_name;
    int start_floor = 0;
    int end_floor = 0;
    int retry_count = 0;
};

string trim_text(const string& raw_text) {
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

vector<string> split_text(const string& line_text, char divider) {
    vector<string> pieces;
    string part_text;
    stringstream line_reader(line_text);

    while (getline(line_reader, part_text, divider)) {
        pieces.push_back(part_text);
    }

    return pieces;
}

int connect_loopback(int chosen_port) {
    int link_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (link_fd < 0) {
        throw runtime_error("Could not create socket");
    }

    sockaddr_in host_box{};
    host_box.sin_family = AF_INET;
    host_box.sin_port = htons(chosen_port);

    if (inet_pton(AF_INET, "127.0.0.1", &host_box.sin_addr) <= 0) {
        close(link_fd);
        throw runtime_error("Could not read loopback address");
    }

    if (connect(link_fd, reinterpret_cast<sockaddr*>(&host_box), sizeof(host_box)) < 0) {
        close(link_fd);
        throw runtime_error("Could not connect to local API");
    }

    return link_fd;
}

void push_full_text(int link_fd, const string& packet_text) {
    size_t sent_total = 0;

    while (sent_total < packet_text.size()) {
        ssize_t sent_now = send(
            link_fd,
            packet_text.data() + sent_total,
            packet_text.size() - sent_total,
            0
        );

        if (sent_now < 0) {
            throw runtime_error("Could not send request");
        }

        sent_total += static_cast<size_t>(sent_now);
    }
}

string collect_full_text(int link_fd) {
    string reply_text;
    char inbox_box[4096];

    while (true) {
        ssize_t bytes_now = recv(link_fd, inbox_box, sizeof(inbox_box), 0);
        if (bytes_now < 0) {
            throw runtime_error("Could not receive response");
        }
        if (bytes_now == 0) {
            break;
        }
        reply_text.append(inbox_box, static_cast<size_t>(bytes_now));
    }

    return reply_text;
}

HttpReply unpack_http_reply(const string& raw_reply) {
    size_t gap_spot = raw_reply.find("\r\n\r\n");
    if (gap_spot == string::npos) {
        throw runtime_error("Bad HTTP reply");
    }

    string head_text = raw_reply.substr(0, gap_spot);
    string body_text = raw_reply.substr(gap_spot + 4);

    istringstream head_reader(head_text);
    string first_row;
    getline(head_reader, first_row);
    if (!first_row.empty() && first_row.back() == '\r') {
        first_row.pop_back();
    }

    istringstream first_row_reader(first_row);
    string http_tag;
    HttpReply ready_reply;
    first_row_reader >> http_tag >> ready_reply.code;
    ready_reply.body_text = trim_text(body_text);
    return ready_reply;
}

HttpReply send_http_call(const string& method_name, int chosen_port, const string& route_text) {
    int link_fd = connect_loopback(chosen_port);

    ostringstream packet_writer;
    packet_writer << method_name << " " << route_text << " HTTP/1.1\r\n";
    packet_writer << "Host: 127.0.0.1:" << chosen_port << "\r\n";
    packet_writer << "Connection: close\r\n";
    packet_writer << "\r\n";

    push_full_text(link_fd, packet_writer.str());
    string raw_reply = collect_full_text(link_fd);
    close(link_fd);

    return unpack_http_reply(raw_reply);
}

class RideInbox {
public:
    void push_call(const RideCall& new_call) {
        lock_guard<mutex> lock(box_lock);
        call_line.push_back(new_call);
        wake_signal.notify_one();
    }

    bool pop_call(RideCall& pulled_call, chrono::milliseconds wait_span) {
        unique_lock<mutex> lock(box_lock);
        wake_signal.wait_for(lock, wait_span, [this]() {
            return closed_box || !call_line.empty();
        });

        if (call_line.empty()) {
            return false;
        }

        pulled_call = call_line.front();
        call_line.pop_front();
        return true;
    }

    void close_box() {
        lock_guard<mutex> lock(box_lock);
        closed_box = true;
        wake_signal.notify_all();
    }

    bool empty_box() {
        lock_guard<mutex> lock(box_lock);
        return call_line.empty();
    }

private:
    deque<RideCall> call_line;
    mutex box_lock;
    condition_variable wake_signal;
    bool closed_box = false;
};

class SchedulerCore {
public:
    SchedulerCore(string building_path, int api_port)
        : building_file_path(std::move(building_path)), port_number(api_port) {
        load_building_file();
    }

    int run_main_loop() {
        cout << "Scheduler is preparing the elevator simulation." << endl;

        HttpReply start_reply = send_http_call("PUT", port_number, "/Simulation/start");
        if (start_reply.code != 200 && start_reply.code != 202) {
            cerr << "Could not start simulation: " << start_reply.body_text << endl;
            return 1;
        }

        scheduler_live.store(true);

        thread pulse_thread(&SchedulerCore::status_worker, this);
        thread intake_thread(&SchedulerCore::input_worker, this);
        thread assign_thread(&SchedulerCore::dispatch_worker, this);

        intake_thread.join();
        assign_thread.join();
        pulse_thread.join();

        cout << "Scheduler finished cleanly." << endl;
        return 0;
    }

private:
    string building_file_path;
    int port_number = 0;

    vector<LiftPlan> lift_table;
    map<string, LiftPulse> pulse_table;
    map<string, int> pending_table;

    mutex pulse_lock;
    RideInbox local_inbox;

    atomic<bool> scheduler_live{false};
    atomic<bool> simulation_finished{false};
    atomic<bool> intake_done{false};

    void load_building_file() {
        ifstream file_reader(building_file_path);
        if (!file_reader.is_open()) {
            throw runtime_error("Could not open building file");
        }

        string row_text;
        while (getline(file_reader, row_text)) {
            row_text = trim_text(row_text);
            if (row_text.empty()) {
                continue;
            }

            vector<string> columns = split_text(row_text, '\t');
            if (columns.size() != 5) {
                throw runtime_error("Bad building file format");
            }

            LiftPlan one_lift;
            one_lift.bay_name = trim_text(columns[0]);
            one_lift.floor_low = stoi(trim_text(columns[1]));
            one_lift.floor_high = stoi(trim_text(columns[2]));
            one_lift.launch_floor = stoi(trim_text(columns[3]));
            one_lift.rider_limit = stoi(trim_text(columns[4]));
            lift_table.push_back(one_lift);

            LiftPulse open_pulse;
            open_pulse.current_floor = one_lift.launch_floor;
            open_pulse.travel_mark = 'S';
            open_pulse.rider_count = 0;
            open_pulse.open_slots = one_lift.rider_limit;
            open_pulse.has_fresh_data = false;

            pulse_table[one_lift.bay_name] = open_pulse;
            pending_table[one_lift.bay_name] = 0;
        }
    }

    bool range_match(const LiftPlan& one_lift, const RideCall& one_call) const {
        return one_lift.floor_low <= one_call.start_floor &&
               one_call.start_floor <= one_lift.floor_high &&
               one_lift.floor_low <= one_call.end_floor &&
               one_call.end_floor <= one_lift.floor_high;
    }

    int direction_hint(const RideCall& one_call) const {
        if (one_call.end_floor > one_call.start_floor) {
            return 1;
        }
        if (one_call.end_floor < one_call.start_floor) {
            return -1;
        }
        return 0;
    }

    int score_lift_choice(const LiftPlan& one_lift, const LiftPulse& one_pulse, int pending_jobs, const RideCall& one_call) const {
        int ride_way = direction_hint(one_call);
        int travel_gap = abs(one_pulse.current_floor - one_call.start_floor);
        int busy_cost = one_pulse.rider_count * 3 + pending_jobs * 4;
        int motion_cost = 0;

        if (one_pulse.travel_mark == 'U') {
            if (!(ride_way >= 0 && one_pulse.current_floor <= one_call.start_floor)) {
                motion_cost += 6;
            }
        } else if (one_pulse.travel_mark == 'D') {
            if (!(ride_way <= 0 && one_pulse.current_floor >= one_call.start_floor)) {
                motion_cost += 6;
            }
        }

        int direct_bonus = 0;
        if (one_pulse.current_floor == one_call.start_floor) {
            direct_bonus -= 3;
        }

        int span_cost = (one_lift.floor_high - one_lift.floor_low) / 10;
        return travel_gap * 2 + busy_cost + motion_cost + span_cost + direct_bonus;
    }

    string choose_lift_for_call(const RideCall& one_call) {
        lock_guard<mutex> lock(pulse_lock);

        string best_bay;
        int best_score = 0;
        bool found_one = false;

        for (const LiftPlan& one_lift : lift_table) {
            if (!range_match(one_lift, one_call)) {
                continue;
            }

            const LiftPulse& one_pulse = pulse_table[one_lift.bay_name];
            int plan_score = score_lift_choice(
                one_lift,
                one_pulse,
                pending_table[one_lift.bay_name],
                one_call
            );

            if (!found_one || plan_score < best_score ||
                (plan_score == best_score && one_lift.bay_name < best_bay)) {
                best_bay = one_lift.bay_name;
                best_score = plan_score;
                found_one = true;
            }
        }

        return best_bay;
    }

    void mark_pending_add(const string& bay_name, int step) {
        lock_guard<mutex> lock(pulse_lock);
        pending_table[bay_name] += step;
        if (pending_table[bay_name] < 0) {
            pending_table[bay_name] = 0;
        }
    }

    void refresh_status_for_lift(const LiftPlan& one_lift) {
        HttpReply state_reply = send_http_call("GET", port_number, "/ElevatorStatus/" + one_lift.bay_name);
        if (state_reply.code != 200) {
            return;
        }

        vector<string> pieces = split_text(state_reply.body_text, '|');
        if (pieces.size() != 5) {
            return;
        }

        LiftPulse fresh_pulse;
        fresh_pulse.current_floor = stoi(trim_text(pieces[1]));
        string mark_text = trim_text(pieces[2]);
        fresh_pulse.travel_mark = mark_text.empty() ? 'S' : mark_text[0];
        fresh_pulse.rider_count = stoi(trim_text(pieces[3]));
        fresh_pulse.open_slots = stoi(trim_text(pieces[4]));
        fresh_pulse.has_fresh_data = true;

        lock_guard<mutex> lock(pulse_lock);
        pulse_table[one_lift.bay_name] = fresh_pulse;

        if (pending_table[one_lift.bay_name] > fresh_pulse.open_slots + fresh_pulse.rider_count) {
            pending_table[one_lift.bay_name] = fresh_pulse.open_slots + fresh_pulse.rider_count;
        }
    }

    void status_worker() {
        while (scheduler_live.load()) {
            try {
                HttpReply sim_reply = send_http_call("GET", port_number, "/Simulation/status");
                string clean_text = trim_text(sim_reply.body_text);

                if (clean_text.find("complete") != string::npos || clean_text.find("stopped") != string::npos) {
                    simulation_finished.store(true);
                }

                if (clean_text.find("running") != string::npos) {
                    for (const LiftPlan& one_lift : lift_table) {
                        refresh_status_for_lift(one_lift);
                    }
                }
            } catch (const exception&) {
                if (simulation_finished.load()) {
                    break;
                }
            }

            if (simulation_finished.load() && intake_done.load() && local_inbox.empty_box()) {
                scheduler_live.store(false);
                local_inbox.close_box();
                break;
            }

            this_thread::sleep_for(chrono::milliseconds(250));
        }
    }

    void input_worker() {
        while (true) {
            try {
                if (simulation_finished.load()) {
                    break;
                }

                HttpReply next_reply = send_http_call("GET", port_number, "/NextInput");
                if (next_reply.code == 200) {
                    string clean_text = trim_text(next_reply.body_text);

                    if (clean_text == "NONE") {
                        this_thread::sleep_for(chrono::milliseconds(150));
                    } else {
                        vector<string> pieces = split_text(clean_text, '|');
                        if (pieces.size() == 3) {
                            RideCall new_call;
                            new_call.rider_name = trim_text(pieces[0]);
                            new_call.start_floor = stoi(trim_text(pieces[1]));
                            new_call.end_floor = stoi(trim_text(pieces[2]));
                            local_inbox.push_call(new_call);
                        }
                    }
                } else {
                    this_thread::sleep_for(chrono::milliseconds(150));
                }
            } catch (const exception&) {
                this_thread::sleep_for(chrono::milliseconds(200));
            }
        }

        intake_done.store(true);
        local_inbox.close_box();
    }

    bool try_assign_call(RideCall& one_call) {
        string chosen_bay = choose_lift_for_call(one_call);
        if (chosen_bay.empty()) {
            return false;
        }

        HttpReply add_reply = send_http_call(
            "PUT",
            port_number,
            "/AddPersonToElevator/" + one_call.rider_name + "/" + chosen_bay
        );

        if (add_reply.code == 200) {
            mark_pending_add(chosen_bay, 1);
            cout << "Assigned " << one_call.rider_name << " to " << chosen_bay
                 << " from floor " << one_call.start_floor
                 << " to floor " << one_call.end_floor << endl;
            return true;
        }

        string clean_text = trim_text(add_reply.body_text);
        if (clean_text.find("already assigned") != string::npos) {
            return true;
        }

        if (clean_text.find("does not exist") != string::npos) {
            cerr << "Skipped rider " << one_call.rider_name << ": " << clean_text << endl;
            return true;
        }

        return false;
    }

    void dispatch_worker() {
        while (true) {
            if (simulation_finished.load() && intake_done.load() && local_inbox.empty_box()) {
                scheduler_live.store(false);
                break;
            }

            RideCall next_call;
            if (!local_inbox.pop_call(next_call, chrono::milliseconds(250))) {
                continue;
            }

            try {
                bool placed = try_assign_call(next_call);
                if (!placed) {
                    next_call.retry_count++;
                    if (next_call.retry_count < 8) {
                        this_thread::sleep_for(chrono::milliseconds(120));
                        local_inbox.push_call(next_call);
                    } else {
                        cerr << "Dropped rider after several retries: "
                             << next_call.rider_name << endl;
                    }
                }
            } catch (const exception&) {
                next_call.retry_count++;
                if (next_call.retry_count < 8) {
                    this_thread::sleep_for(chrono::milliseconds(120));
                    local_inbox.push_call(next_call);
                }
            }
        }
    }
};

int read_port_number(const string& port_text) {
    int ready_port = stoi(port_text);
    if (ready_port < 1024 || ready_port > 65535) {
        throw runtime_error("Port must be between 1024 and 65535");
    }
    return ready_port;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        cerr << "Usage: scheduler_os <path_to_building_file> <port_number>" << endl;
        return 1;
    }

    try {
        SchedulerCore app_shell(argv[1], read_port_number(argv[2]));
        return app_shell.run_main_loop();
    } catch (const exception& problem) {
        cerr << "Scheduler error: " << problem.what() << endl;
        return 1;
    }
}

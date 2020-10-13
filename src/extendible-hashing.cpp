#include "extendible-hashing.h"
#include "bucket.h"

#include <iostream>
#include <vector>
#include <string.h>
#include <sstream>
#include <ncurses.h>
#include <curses.h>

namespace db {

Hashing::Hashing (int size) {
    levels = 0;
    num_entries = 1;
    bucket_addr = new int [num_entries];
    max_keys = size;
    file_name = new char;
    dir_buffer = new file::length_field_buffer;
    dir_file = new file::buffer_file(*dir_buffer);
    current_bucket = new db::bucket(*this, size);
    print_bucket = new db::bucket(*this, size);
    bkt_buffer = new db::bucket_buffer (12, size);
    bucket_file = new file::buffer_file_bucket(*bkt_buffer);
}

Hashing::~Hashing() {
    close();
}


void make_names (char *name, char *&dir_name, char *&bucket_name) {
    std::ostringstream tmp;
    tmp << name << ".dir" << std::ends;
    dir_name = strdup(tmp.str().c_str());

    std::ostringstream b_tmp;
    b_tmp << name << ".bkt" << std::ends;
    bucket_name = strdup(b_tmp.str().c_str());
}

int Hashing::open(char *name) {
    char *dir_name, *bucket_name;
    int result;

    make_names(name, dir_name, bucket_name);

    file_name = dir_name;

    result = dir_file->open(dir_name);

    if (!result) return 0;

    result = dir_file->read();

    if (result == -1) return 0;

    result = unpack();

    if (result == -1) return 0;

    result = bucket_file->open(bucket_name);

    return result;
}

int Hashing::create(char *name) {
    char *dir_name, *bucket_name;

    make_names(name, dir_name, bucket_name);

    int result = dir_file->create(dir_name);

    if (!result) return 0;

    file_name = dir_name;
    indexed_files.push_back(name);
    result = bucket_file->create(bucket_name);

    if (!result) return 0;

    bucket_addr[0] = store_bucket(current_bucket);;

    return result;
}

int Hashing::close() {
    int result = pack();

    if (result == -1) return 0;

    dir_file->rewind();
    result = dir_file->write();

    if (result == -1) return 0;

    return dir_file->close() && bucket_file->close();
}

int Hashing::insert(char *key, int addr) {
    if (search(key) == -1) return current_bucket->insert(key, addr);

    return 0;
}

int Hashing::remove(char *key) {
    int bucket_addr = find(key);

    load_bucket(current_bucket, bucket_addr);

    return current_bucket->remove(key);
}

int Hashing::search(char *key) {
    int bucket_addr = find(key);

    load_bucket(current_bucket, bucket_addr);

    return current_bucket->search(key);
}

int Hashing::get_indexed_files_names () {
    if (indexed_files.size() == 0) return -1;

    for (auto i : indexed_files) {
        std::cout << i << "\n";
    }

    return 1;
}

std::vector<char *> Hashing::get_indexed_files() {
    if (indexed_files.size() == 0) return {};

    return indexed_files;
}

int Hashing::hash (char *key) {
    int sum = 0;
    int  len = strlen(key);

    if (len % 2 == 1) len ++;

    for (int i = 0; i < len; i += 2) {
        sum = (sum + 100 * key[i] + key[i + 1]) % 19937;
    }

    return sum;
}

std::ostream &Hashing::print(std::ostream &stream) {
    stream << "hash levels " << levels << " size " << num_entries << std::endl;

    for (int i = 0; i < num_entries; ++i) {
        stream << "bucket " << bucket_addr[i] << " addr " << (void *)i << std::endl;

        load_bucket(print_bucket, bucket_addr[i]);
        print_bucket->print(stream);
    }

    stream << "end" << std::endl;

    return stream;
}

void Hashing::get_data (WINDOW *wnd) {
		for (int i = 0; i < num_entries; ++i) {
			load_bucket(print_bucket, bucket_addr[i]);
			print_bucket->get_data(wnd);
		}
}

    void Hashing::get_data () {
        for (int i = 0; i < num_entries; ++i) {
            int tmp = bucket_addr[i];
            load_bucket(print_bucket, bucket_addr[i]);
            print_bucket->get_data();
        }
    }


int Hashing::make_addr (char *key, int levels) {
    int addr = 0;
    int mask = 1;
    int hashing = hash(key);

    for (int i = 0; i < levels; ++i) {
        addr = addr << 1;
        int lbit = hashing & mask;
        addr = addr | lbit;
        hashing = hashing >> 1;
    }

    return addr;
}

int Hashing::double_size() {
    int new_size = 2 * num_entries;
    int *new_bucket_addr = new int[new_size];

    for (int i = 0; i < num_entries; ++i) {
        new_bucket_addr[2 * i] = bucket_addr[i];
        new_bucket_addr[2 * i + 1] = bucket_addr[i];
    }

    delete bucket_addr;
    bucket_addr = new_bucket_addr;
    levels ++;
    num_entries = new_size;

    return 1;
}

int Hashing::collapse() {
    if (levels == 0) return 0;

    int new_size = num_entries / 2;
    int *new_address = new int [new_size];

    for (int i = 0; i < num_entries; i += 2) {
        if (bucket_addr[i] != bucket_addr[i + 1])
            return 0;
    }

    for (int i = 0; i < new_size; ++i) {
        new_address[i] = bucket_addr[i * 2];
    }

    delete bucket_addr;
    bucket_addr = new_address;
    levels --;
    num_entries = new_size;

    return 1;
}

int Hashing::insert_bucket(int bucket_addr, int first, int last) {
    for (int i = 0; i <= last; ++i) {
        this->bucket_addr[i] = bucket_addr;
    }

    return 1;
}

int Hashing::remove_bucket(int bucket_ind, int bucket_levels) {
    int fill_bits = levels - bucket_levels;
    int buddy_ind = bucket_ind ^ (1 << (fill_bits - 1));
    int new_bucket_addr = bucket_addr[buddy_ind];
    int lind = bucket_ind >> fill_bits << fill_bits;
    int hind = lind + (1 << fill_bits) - 1;

    for (int i = lind; i <= hind; ++i) {
        bucket_addr[i] = new_bucket_addr;
    }

    return 0;
}

int Hashing::find(char *key) {
    return bucket_addr[make_addr(key, levels)];
}

int Hashing::store_bucket(db::bucket *bkt) {
    int result = bkt_buffer->pack(*bkt);
    int addr = bkt->bucket_addr;

    if (result == -1) return -1;

    if (addr != 0) return bucket_file->write(addr);

    addr = bucket_file->append();
    bkt->bucket_addr = addr;

    return addr;
}

int Hashing::load_bucket(db::bucket *bkt, int bucket_addr) {
    int result = bucket_file->read(bucket_addr);

    if (result == -1) return 0;

    result = bkt_buffer->unpack(*bkt);

    if (result == -1) return 0;

    bkt->bucket_addr = bucket_addr;

    return 1;
}

int Hashing::pack() const {
    int result, pack_size;

    dir_buffer->clear();

    pack_size =dir_buffer->pack(&levels, sizeof(int));

    if (pack_size == -1) return -1;

    for (int i = 0; i < num_entries; ++i) {
        result = dir_buffer->pack(&bucket_addr[i], sizeof(int));

        if (result == -1) return -1;

        pack_size += result;
    }

    return pack_size;
}

int Hashing::unpack() {
    int result = dir_buffer->unpack(&levels, sizeof(int));

    if (result == -1) return -1;

    num_entries = 1 << levels;

    if (bucket_addr != 0) delete bucket_addr;

    bucket_addr = new int[num_entries];

    for (int i = 0; i < num_entries; ++i) {
        result = dir_buffer->unpack(&bucket_addr[i], sizeof(int));

        if (result == -1) return -1;
    }

    return 0;
}

}

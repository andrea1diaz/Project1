#include <bucket.h>
#include <iostream>
#include <fstream>
#include <string.h>
#include <ncurses.h>
#include <curses.h>

#include "extendible-hashing.cpp"

namespace db {
    int num_k = 0;
bucket::bucket(db::Hashing &dir, int size) : num_keys (0), keys (0), rec_addr (0), dir(dir) {
    this->unique = unique != 0;
    bucket_addr = 0;
    levels = 0;

    if (size <= 0) {
        max_keys = 0;
    }

    else {
        max_keys = size;
        keys = new char *[size];
        rec_addr = new int [size];
    }
}

int bucket::insert (char *key, int addr) {
    if (num_keys < max_keys) {
        simple_insert(key, addr);

        dir.store_bucket(this);

        return 1;
    }

    else {
        split();

        return dir.insert(key, addr);
    }
}


int bucket::simple_insert(const char *key, int addr) {
    int ind = find(key);
    int i = num_keys - 1;

    if (unique && ind >= 0) return 0;
    if (num_keys == max_keys) return 0;

    for (; i >= 0; --i) {
        int eval = strcmp(key, keys[i]);

        if (eval > 0) break;

        keys[i + 1] = keys[i];
        rec_addr[i + 1] = rec_addr[i];
    }

    keys[i + 1] = strdup(key);
    rec_addr[i + 1] = addr;
    num_keys ++;

    return 1;
}


int bucket::remove (const char *key) {
    simple_remove(key);

    check_combine();
    dir.store_bucket(this);

    return 1;
}


int bucket::simple_remove(const char *key) {
    int ind = find(key);

    if (ind < 0) return 0;

    for (int i = ind; i < num_keys; ++i) {
        keys[i] = keys[i + 1];
        rec_addr[i] = rec_addr[i + 1];
    }

    num_keys --;

    return 1;
}

int bucket::search (const char *key) {
    int ind = find(key);

    if (ind < 0) return ind; // not found

    return rec_addr[ind];
}

int bucket::redistribute(bucket &new_bucket) {
    for (int i = num_keys - 1; i >= 0; --i) {
        int bucket_addr = dir.find(keys[i]);

        if (bucket_addr != this->bucket_addr) {
            new_bucket.simple_insert(keys[i], rec_addr[i]);
            simple_remove (keys[i]);
        }
    }

    return 1;
}

bucket* bucket::split() {
    int start, end;

    if (levels == dir.levels) dir.double_size();

    bucket *new_bucket = new bucket (dir, max_keys);

    dir.store_bucket (new_bucket);

    int shared_addr = make_addr (keys[0], levels);
    int to_fill = dir.levels - (levels + 1);

    start = end = (shared_addr << 1) | 1;

    for (int i = 0; i < to_fill; ++i) {
        start = start << 1;
        end = (end << 1) | 1;
    }

    dir.insert_bucket (new_bucket->bucket_addr, start, end);
    levels ++;
    new_bucket->levels = levels;
    redistribute(*new_bucket);
    dir.store_bucket(this);
    dir.store_bucket(new_bucket);

    return new_bucket;
}

int bucket::find (const char *key) {
    for (int i = 0; i < num_keys; ++i) {
        int eval = strcmp(keys[i], key);

        if (eval == 0) return i;
        else if (eval > 0) return -1;
    }

    return -1;
}


int bucket::combine(bucket *buddy, int buddy_ind) {
    for (int i = 0; i < buddy->num_keys; ++i) {
        int check = insert(buddy->keys[i], buddy->rec_addr[i]);

        if (!check) return 0;
    }

    levels --;
    dir.remove_bucket(buddy_ind, levels);

    return 1;
}


int bucket::check_combine () {
    int buddy_ind = find_buddy();

    if (buddy_ind == -1) return 0;

    int buddy_addr = dir.bucket_addr[buddy_ind];
    bucket *buddy_bucket = new bucket (dir, max_keys);
    dir.load_bucket (buddy_bucket, buddy_addr);

    if (num_keys + buddy_bucket->num_keys > max_keys) return 0;

    combine(buddy_bucket, buddy_ind);

    if (dir.collapse()) check_combine();

    return 1;
}


int bucket::find_buddy () {
    if (dir.levels == 0) return -1;
    if (levels < dir.levels) return -1;

    int shared_addr = make_addr (keys[0], levels);

    return shared_addr ^ 1;
}

int bucket::hash (char *key) {
    int sum = 0;
    int  len = strlen(key);

    if (len % 2 == 1) len ++;

    for (int i = 0; i < len; i += 2) {
        sum = (sum + 100 * key[i] + key[i + 1]) % 19937;
    }

    return sum;
}

int bucket::make_addr (char *key, int levels) {
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

std::ostream &bucket::print(std::ostream &stream) {
    stream << "Bucket levels: " << levels << std::endl;
    stream << "Text Index max keys: " << max_keys << " present keys " << num_keys << std::endl;

    for (int i = 0; i < num_keys; ++i) {
        stream << "\tKey[" << i << "] = " << keys[i] << " " << rec_addr[i] << std::endl;
    }

    return stream;
}


void bucket::get_data(WINDOW *wnd) {
	mvwprintw(wnd, 2, 1, "%d", num_keys);
	for (int i = 0; i < num_keys; ++i) {
	  mvwprintw(wnd, i + 1, 1, keys[i]);
	}
	wrefresh(wnd);
}

    void bucket::get_data() {
        for (int i = 0; i < num_keys; ++i) {
           std::cout << keys[i] << "\n";
        }
    }

}

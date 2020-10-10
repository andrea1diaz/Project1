#include <bucket.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string.h>
#include "extendible-hashing.cpp"

namespace db {
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
        int ind = find(key);
        int i = num_keys - 1;

        if (unique && ind >= 0) return 0;

        for (; i >= 0; --i) {
            int eval = strcmp(key, keys[i]);

            if (eval > 0) break;

            keys[i + 1] = keys[i];
            rec_addr[i + 1] = rec_addr[i];
        }

        keys[i + 1] = strdup(key);
        rec_addr[i + 1] = addr;
        num_keys ++;
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
    int ind = find(key);

    if (ind < 0) return 0;

    for (int i = ind; i < num_keys; ++i) {
        keys[i] = keys[i + 1];
        rec_addr[i] = rec_addr[i + 1];
    }

    num_keys --;
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

bucket_buffer::bucket_buffer(int k_size, int size) {
    initialized = false;
    int field_max = 1 + 2 * size + 1;
    int max_b = sizeof(int) + size * k_size * sizeof(int) + sizeof(int);

    if (field_max < 0) this->field_max = 0;
    else this->field_max = field_max;

    field_size = new int[field_max];
    buffer_size_max = max_b;
    buffer_size = 0;
    buffer = new char[buffer_size_max];
    field_num = 0;
    this->k_size = k_size;
    this->k_max = size;

    add_field (sizeof(int));

    for (int i = 0; i < size; ++i) {
        add_field (k_size);
        add_field (sizeof(int));
    }

    dummy = new char[k_size + 1];

    clear();
}

void bucket_buffer::clear() {
    next_byte = 0;
    next_field = 0;
    buffer[0] = 0;
    packing = true;
}


    int bucket_buffer::read(std::fstream &stream) {
        if (stream.eof()) return -1;

        int addr = stream.tellg();
        unsigned short bff_size;

        clear();

        stream.read((char *) &bff_size, sizeof(bff_size));

        if (!stream.good()) {
            stream.clear();
            return -1;
        }

        buffer_size = bff_size;

        if (buffer_size > buffer_size_max) return -1;

        stream.read(buffer, buffer_size);

        if (!stream.good()) {
            stream.clear();
            return -1;
        }

        return addr;
    }

    int bucket_buffer::dread(std::fstream &stream, int addr) {
        stream.seekg(addr, std::ios::beg);

        if (stream.tellp() != addr) return -1;

        return read(stream);
    }

    int bucket_buffer::write(std::fstream &stream) {
        int addr = stream.tellp();
        unsigned short bff_size = buffer_size;

        stream.write((char *) &bff_size, sizeof(bff_size));

        if (!stream) return -1;

        stream.write(buffer, buffer_size);

        if (!stream.good()) return -1;

        return addr;
    }

    int bucket_buffer::dwrite(std::fstream &stream, int addr) {
        stream.seekg(addr, std::ios::beg);

        if (stream.tellp() != addr) return -1;

        return write(stream);
    }

int bucket_buffer::add_field(int field_size) {
    initialized = 1;

    if (field_num == field_max) return 0;

    if (buffer_size + field_size > buffer_size_max) return 0;

    this->field_size[field_num] = field_size;
    field_num ++;
    buffer_size += field_size;

    return 1;
}

int bucket_buffer::pack(const bucket &bkt) {
    clear();

    int result = pack(&bkt.num_keys, -1);

    for (int i = 0; i < bkt.num_keys; ++i) {
        result = result && pack(bkt.keys[i]);
        result = result && pack(&bkt.rec_addr[i]);
    }

    for (int i = 0; i < bkt.max_keys - bkt.num_keys; ++i) {
        result = result && pack(dummy);
        result = result && pack(dummy);
    }

    return result;
}

int bucket_buffer::pack(const void *field, int sz) {
    if (next_field == field_num || !packing) return -1;

    int start = next_byte;
    int pack_size = field_size[next_field];

    if (sz != -1 && pack_size != sz)  return -1;

    memcpy(&buffer[start], field, pack_size);

    next_byte += pack_size;
    next_field ++;

    if (next_field == field_num) {
        packing = -1;
        next_field = next_byte = 0;
    }

    return pack_size;
}

int bucket_buffer::unpack(bucket &bkt) {
    int result = unpack(&bkt.num_keys);

    for (int i = 0; i < bkt.num_keys; ++i) {
        bkt.keys[i] = new char[k_size];
        result = result && unpack(bkt.keys[i]);
        result = result && unpack(&bkt.rec_addr[i]);
    }

    for (int i = 0; i < bkt.max_keys - bkt.num_keys; ++i) {
        result = result && unpack(dummy);
        result = result && unpack(dummy);
    }

    return result;
}

int bucket_buffer::unpack(void *field, int sz) {
    packing = 0;

    if (next_field == field_num) return -1;

    int start = next_byte;
    int pack_size = field_size[next_field];

    memcpy(field, &buffer[start], pack_size);

    next_byte += pack_size;
    next_field ++;

    if (next_field == field_num) clear();

    return pack_size;
}

    static const char *header_str_b = "BucketBuffer";
    static const int header_size_b = strlen(header_str_b);
    const char *header_str = "Bucket";
    const int header_size = strlen(header_str);


    int bucket_buffer::read_header(std::fstream &stream) {
        int result;
        char str[header_size + 1];
        char str_s[header_size_b + 1];

        stream.seekg(0, std::ios::beg);
        stream.read(str_s, header_size_b);

        if (!stream.good()) result = -1;

        if (strncmp(str_s, header_str_b, header_size_b) == 0) result = header_size_b;
        else result = -1;

        if (!result) return 0;

        stream.read(str, header_size);

        if (!stream.good()) return false;

        if (strncmp(str, header_str, header_size) != 0) return 0;

        return stream.tellg();
    }

    int bucket_buffer::write_header(std::fstream &stream) {
        int result;

        stream.seekg(0, std::ios::beg);
        stream.write(header_str_b, header_size_b);

        if (!stream.good()) result = -1;
        else result = header_size_b;

        if (!result) return 0;

        stream.write(header_str, header_size);

        if (!stream.good()) return 0;

        return stream.tellp();
    }

}
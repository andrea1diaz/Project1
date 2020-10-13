#include <bucketbuffer.h>
#include <bucket.h>
#include <fstream>
#include <iostream>

namespace db {
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
    k_num = 0;

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

        clear();

        unsigned short bff_size;
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

        stream.write((char *)buffer, buffer_size);

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

    if (result == -1) return -1;

    return pack(&bkt.levels);
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

    if (result == -1) return -1;

    return unpack(&bkt.levels);
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

    int bucket_buffer::read_header(std::fstream &stream) {
        char *str_s = new char [header_size_b + 1];

        stream.seekg(0, std::ios::beg);
        stream.read(str_s, header_size_b);
        stream.tellg();

        if (!stream.good()) return -1;

        stream.read((char *) &buffer_size_max, sizeof(buffer_size_max));

        if (!stream.good()) return -1;

        if (strncmp(str_s, header_str_b, header_size_b) != 0) return -1;

        stream.read((char *) &field_num, sizeof(field_num));

        if (!stream.good()) return -1;

        field_size = new int[field_num];

        for (int i = 0; i < field_num; ++i) {
            stream.read((char *) &field_size[i], sizeof(field_size[i]));
        }

        return stream.tellg();
    }

    int bucket_buffer::write_header(std::fstream &stream) {

        stream.seekp(0, std::ios::beg);
        stream.write(header_str_b, header_size_b);

        if (!stream.good()) return -1;

        stream.write((char *) &buffer_size_max, sizeof(buffer_size_max));

        if (!stream.good()) return -1;

        stream.write((char *) &field_num, sizeof(field_num));

        for (int i = 0; i < field_num; ++i)
            stream.write((char *) &field_size[i], sizeof(field_size[i]));

        if (!stream.good()) return -1;

        return stream.tellp();
    }
}

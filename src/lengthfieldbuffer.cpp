#include "lengthfieldbuffer.h"

#include <string.h>

namespace file {

length_field_buffer::length_field_buffer(int size) {
    initialized = true;

    if (size < 0) this->size = 0;

    this->size = size;
    buffer = new char[size];
    buffer_size = 0;

    clear();
}

void length_field_buffer::clear() {
    next = 0;
    packing = true;
}

int length_field_buffer::read(std::fstream &stream) {
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

    if (buffer_size > size) return -1;

    stream.read(buffer, buffer_size);

    if (!stream.good()) {
        stream.clear();
        return -1;
    }

    return addr;
}

int length_field_buffer::dread(std::fstream &stream, int addr) {
    stream.seekg(addr, std::ios::beg);

    if (stream.tellp() != addr) return -1;

    return read(stream);
}

int length_field_buffer::write(std::fstream &stream) {
    int addr = stream.tellp();
    unsigned short bff_size = buffer_size;

    stream.write((char *) &bff_size, sizeof(bff_size));

    if (!stream) return -1;

    stream.write(buffer, buffer_size);

    if (!stream.good()) return -1;

    return addr;
}

int length_field_buffer::dwrite(std::fstream &stream, int addr) {
    stream.seekg(addr, std::ios::beg);

    if (stream.tellp() != addr) return -1;

    return write(stream);
}

int length_field_buffer::pack(const void *field, int size) {
    short len;
    int start = next;

    if (size >= 0) len = size;
    else len = strlen((char *) field);

    next += len + sizeof(len);

    if (next > size) return -1;

    memcpy(&buffer[start], &len, sizeof(len));
    memcpy(&buffer[start + sizeof(len)], field, len);

    buffer_size = next;

    return len;
}

int length_field_buffer::unpack(void *field, int size) {
    short len;
    int start = next;

    if (next >= buffer_size) return -1;

    memcpy(&len, &buffer[start], sizeof(len));

    if (size != -1 && len > size) return -1;

    next += len + sizeof(len);

    if (next > buffer_size) return -1;

    memcpy(field, &buffer[start + sizeof(len)], len);

    if (size > len || size == -1) ((char *) field)[len] = 0;

    return len;
}

length_field_buffer &length_field_buffer::operator =(const length_field_buffer &bff) {
    if (size < bff.size) return *this;

    initialized = bff.initialized;
    buffer_size = bff.buffer_size;

    memcpy(buffer, bff.buffer, bff.buffer_size);

    next = bff.next;
    packing = bff.packing;

    return *this;
}

static const char *header_str_b = "LFBuffer";
static const int header_size_b = strlen(header_str_b);
const char *header_str = "Length";
const int header_size = strlen(header_str);


int length_field_buffer::read_header(std::fstream &stream) {
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

int length_field_buffer::write_header(std::fstream &stream) {
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
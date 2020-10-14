#pragma once

#include <fstream>

namespace db {
class bucket;

class bucket_buffer {
public:
    bucket_buffer (int k_size, int size);
    void clear ();
    int read (std::fstream &stream);
    int dread (std::fstream &stream, int addr);
    int write (std::fstream &stream);
    int dwrite (std::fstream &stream, int addr);
    int add_field (int field_size);
    int pack (const void *field, int sz = -1);
    int pack (const bucket &bkt);
    int unpack (bucket &bkt);
    int unpack (void *field, int sz = -1);
    int read_header (std::fstream &stream);
    int write_header (std::fstream &stream);

protected:
    bool initialized;
    int packing;
    int *field_size;
    int field_max;
    int field_num;
    int buffer_size;
    int buffer_size_max;
    int k_max;
    int k_size;
    char *dummy;
    char *buffer;
    int next_field;
    int next_byte;

    friend class bucket;
};
}

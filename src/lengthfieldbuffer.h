#pragma once

#include <fstream>

namespace file {
class length_field_buffer {
public:
    length_field_buffer (int size = 1000000);
    void clear ();
    int read (std::fstream &stream);
    int dread (std::fstream &stream, int addr);
    int write (std::fstream &stream);
    int dwrite (std::fstream &stream, int addr);
    int pack (const void *field, int size = -1);
    int unpack (void *field, int size = -1);
    length_field_buffer &operator = (const length_field_buffer &);
    int read_header (std::fstream &stream);
    int write_header (std::fstream &stream);

protected:
    bool initialized;
    bool packing;
    char *buffer;
    int buffer_size;
    int size;
    int next;
};
}

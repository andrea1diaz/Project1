#pragma once

#include "lengthfieldbuffer.h"

#include <fstream>

namespace file {
class buffer_file {
public:
    buffer_file (file::length_field_buffer &);
    int open (char *filename);
    int create (char *filename);
    int close();
    int read (int addr = -1);
    int write (int addr = -1);
    int rewind ();
    int append();
    file::length_field_buffer &get_buffer ();

protected:
    file::length_field_buffer &buffer;

    int header_size;
    std::fstream file;
    int read_header();
    int write_header();
};
}
#pragma once

#include "bucketbuffer.h"

#include <fstream>

namespace file {
class buffer_file_bucket {
public:
    buffer_file_bucket (db::bucket_buffer &);
    int open (char *filename);
        int create (char *filename);
        int close();
        int read (int addr = -1);
        int write (int addr = -1);
        int rewind ();
        int append();
        db::bucket_buffer &get_buffer ();

    protected:
        db::bucket_buffer &buffer;

        int header_size;
        std::fstream file;
        int read_header();
        int write_header();
    };

}

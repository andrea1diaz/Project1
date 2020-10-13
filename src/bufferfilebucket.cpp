#include "bufferfilebucket.h"

namespace file {
    buffer_file_bucket::buffer_file_bucket(db::bucket_buffer &from) : buffer (from) {}

    int buffer_file_bucket::open(char *filename) {
        file.open (filename, std::ios::in | std::ios::out | std::ios::binary);

        if (!file.good()) return false;

        file.seekg(0, std::ios::beg);
        file.seekp(0, std::ios::beg);

        header_size = read_header();

        if (!header_size) return false;

        file.seekp(header_size, std::ios::beg);
        file.seekg(header_size, std::ios::beg);

        return file.good();
    }

    int buffer_file_bucket::create(char *filename) {
        file.open ((const char *)filename, std::ios::out | std::ios::binary | std::ios::app);

        if (!file.good()) {
            file.close();
            file.clear();
            return false;
        }

        header_size = write_header();

        return header_size != 0;
    }

    int buffer_file_bucket::close() {
        file.close();
        file.clear();
        return true;
    }

    int buffer_file_bucket::read(int addr) {
        if (addr == -1) return buffer.read(file);
        else return buffer.dread(file, addr);
    }

    int buffer_file_bucket::write(int addr) {
        if (addr == -1) return buffer.write(file);
        else return buffer.dwrite(file, addr);
    }

    int buffer_file_bucket::rewind() {
        file.seekg(header_size, std::ios::beg);
        file.seekp(header_size, std::ios::beg);

        return 1;
    }

    int buffer_file_bucket::append() {
        file.seekp(0, std::ios::end);

        return buffer.write(file);
    }

    db::bucket_buffer &buffer_file_bucket::get_buffer() {
        return buffer;
    }

    int buffer_file_bucket::read_header() {
        return buffer.read_header (file);
    }

    int buffer_file_bucket::write_header() {
        return buffer.write_header (file);
    }
}
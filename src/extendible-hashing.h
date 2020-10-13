#pragma once

#include "lengthfieldbuffer.h"
#include <bufferfile.h>
#include <bufferfilebucket.h>

#include <vector>
#include <ncurses.h>
#include <curses.h>

namespace db {
class bucket;
class bucket_buffer;

class Hashing {
public:
    Hashing (int size = -1);
    ~Hashing();

    int open (char *name);
    int create (char *name);
    int close ();
    int insert (char *key, int addr);
    int remove (char *key);
    int search (char *key);
    int get_indexed_files_names ();
    std::vector<char *> get_indexed_files ();
    int make_addr (char *key, int levels);
    int hash (char *key);
    std::ostream &print (std::ostream &stream);
		void get_data (WINDOW *wnd);
    void get_data ();

protected:
    int max_keys;
    int levels;
    int num_entries;
    int *bucket_addr;
    char *file_name;
    std::vector<char *> indexed_files;

    int double_size ();
    int collapse ();
    int insert_bucket (int bucket_addr, int first, int last);
    int remove_bucket (int bucket_ind, int levels);
    int find (char *key);
    int store_bucket (db::bucket *bkt);
    int load_bucket (db::bucket *bkt, int bucket_addr);
    int pack () const;
    int unpack ();

    db::bucket *current_bucket;
    db::bucket *print_bucket;
    db::bucket_buffer *bkt_buffer;
    file::buffer_file *dir_file;
    file::buffer_file_bucket *bucket_file;
    file::length_field_buffer *dir_buffer;

    friend class bucket;
};
}


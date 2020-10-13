#pragma once

#include <iostream>
#include <fstream>
#include <ncurses.h>
#include <curses.h>

namespace db {
class Hashing;
class bucket_buffer;

class bucket {
protected:
    bucket (db::Hashing &, int size = 100);
    db::bucket* split ();
    int insert (char *key, int addr);
    int simple_insert (const char *key, int addr);
    int remove (const char *key);
    int simple_remove (const char *key);
    int search (const char *key);
    int redistribute (bucket &new_bucket);
    int find_buddy ();
    int find (const char * key);
    int combine (bucket *buddy, int buddy_ind);
    int check_combine ();
    int make_addr (char *key, int levels);
    int hash (char *key);
    std::ostream &print (std::ostream &stream);
		void get_data (WINDOW *wnd);
    void get_data ();

    int max_keys;
    int num_keys;
    char **keys;
    int *rec_addr;
    int unique;
    int levels;
    int bucket_addr;
    Hashing &dir;

    friend class Hashing;
    friend class bucket_buffer;
};
}

#pragma once

namespace db {
class Hashing;

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


class bucket_buffer {
public:
    bucket_buffer (int k_size, int size);
    void clear ();
    int add_field (int field_size);
    int pack (const void *field, int sz = -1);
    int pack (const bucket &bkt);
    int unpack (bucket &bkt);
    int unpack (void *field, int sz = -1);

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
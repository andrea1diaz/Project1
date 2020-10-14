#include "DataBase.h"

#include <btree.h>
#include <pagemanager.h>

#define PAGE_SIZE  64
#define BTREE_ORDER   ((PAGE_SIZE - (2 * sizeof(long) + sizeof(int) +  2 * sizeof(long)) ) /  (sizeof(int) + sizeof(long)))


DataBase::DataBase(std::shared_ptr<db::disk::pagemanager> pm, char *name) : bt(pm), hash(4) {
    int result = hash.create(name);

    if (result == 0) {
        std::cout << "errror\n";
        return;
    }
}

int DataBase::load_file(char *name) {
    file.open(name, std::ios::in | std::ios::out | std::ios::binary);

    char *buffer = new char[1000000];
    unsigned short bff_size;
    int buffer_size;
    int i = 0;
    int j = 0;

    file.clear();
    file.seekg(0);

    while (file.good()) {
        file.read((char *) &bff_size, sizeof(bff_size));
        buffer_size = bff_size;

        file.read(buffer, bff_size);

        hash.insert(buffer, 100 + i);

        i++;
    }

    file.clear();
    file.seekg(0);

    while (file.good()) {
        file.read((char *) &bff_size, sizeof(bff_size));
        buffer_size = bff_size;

        file.read(buffer, bff_size);
        bt.insert(buffer);

        j++;
    }
}
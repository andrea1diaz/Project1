#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <extendible-hashing.h>
#include <ui.h>

#include <ncurses.h>
#include <curses.h>
#include <fmt/core.h>
#include <iostream>
#include <string>

using namespace std;
db::Hashing hs(4);
struct HashingTest : public ::testing::Test
{

};

TEST_F(HashingTest, First) {

    int result = hs.create((char *) "hash_file");

    if (result == 0) {
        std::cout << "errror\n";
        return;
    }

    char *keys[] = {(char *)"name1", (char *)"name2", (char *)"name3", (char *)"name4", (char *)"name5"};
    int keys_num = 5;

    for (int i = 0; i < keys_num; ++i) {
        std::cout << keys[i] << " " << (void *) hs.hash(keys[i]) << " " << (void *)hs.make_addr(keys[i], 6) << std::endl;
        hs.hash(keys[i]);
        hs.make_addr(keys[i], 6);
        result = hs.insert(keys[i], 100 + i);

        if (result == 0) return;
        hs.print(std::cout);

    }

    return;
};

TEST_F(HashingTest, UI) {
    hs.get_data();
		//ui::database win(hs);
		//win.init();

}

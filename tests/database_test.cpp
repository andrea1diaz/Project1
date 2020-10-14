#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <ctime>
#include <chrono>

#include <DataBase.h>

using namespace std;
struct DataBaseTest : public ::testing::Test
{

};

TEST_F(DataBaseTest, Insert1k) {
    std::shared_ptr<db::disk::pagemanager> pm = std::make_shared<db::disk::pagemanager>((char *)"btree.index", true);
    DataBase db(pm, (char *)"hash1k");

    db.load_file((char *)"games1k.txt");
    auto end = std::chrono::high_resolution_clock::now();
    return;
};

TEST_F(DataBaseTest, Insert10k) {
    std::shared_ptr<db::disk::pagemanager> pm = std::make_shared<db::disk::pagemanager>((char *)"btree.index", true);
    DataBase db(pm, (char *)"hash10k");

    db.load_file((char *)"games10k.txt");

    return;
};

TEST_F(DataBaseTest, Insert20k) {
    std::shared_ptr<db::disk::pagemanager> pm = std::make_shared<db::disk::pagemanager>((char *)"btree.index", true);
    DataBase db(pm, (char *)"hash20k");

    db.load_file((char *)"games20k.txt");

    return;
};

#pragma once

#include "extendible-hashing.h"

#include <ncurses.h>
#include <iostream>
#include <vector>

namespace ui {
class database {
public:
	database(db::Hashing &from);
	
	void init ();
	void file_menu();
	void open_file(char *name);

private:
	int x_max;
	int y_max;
	int file_choice;

	std::vector<char *> hash_files;

	WINDOW *files_window;
	WINDOW *data_window;

	db::Hashing &hash;
};
}


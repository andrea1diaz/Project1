#include "ui.h"

#include <ncurses.h>
#include <curses.h>
#include <vector>

namespace ui {

database::database (db::Hashing &from) : hash(from) {
	hash_files = hash.get_indexed_files();
}

void database::init () {
	initscr();
	noecho();
	cbreak();
	
	getmaxyx (stdscr, y_max, x_max);

	files_window = newwin(y_max - 2, 50, 1, 3);
	data_window = newwin(y_max - 2, x_max - 59, 1, 56);

	box (files_window, 0, 0);
	box (data_window, 0, 0);

	refresh();

	wrefresh(files_window);
	wrefresh(data_window);

	keypad(files_window, true);

	file_menu ();

	getch();
	endwin();
}

void database::file_menu () {
	int highlight = 0;
	while (1) {
		for (int i = 0; i < hash_files.size(); ++i) {
			if (i == highlight) wattron(files_window, A_REVERSE);
			const char *x = hash_files[i];
			mvwprintw(files_window, i + 1, 1, x);
			wattroff(files_window, A_REVERSE);
		}

		file_choice = wgetch(files_window);

		switch (file_choice) {
			case KEY_UP:
				highlight --;
				if (highlight < 0) highlight = 0;

				break;

			case KEY_DOWN:
				highlight ++;
				if (highlight > hash_files.size() - 1) highlight = hash_files.size() - 1;
				break;

			case KEY_RIGHT:
				mvwprintw(data_window, 1, 1, " %s", hash_files[highlight]);
				open_file (hash_files[highlight]);
				wrefresh(data_window);
				break;

			default:
				if (highlight < 0) highlight = 0;
				break;
		}

		if (file_choice == 10) break;
	}	

	//open_file (hash_files[highlight]);
}

void database::open_file (char *name) {
	hash.open (name);

	hash.get_data(data_window);
}

}

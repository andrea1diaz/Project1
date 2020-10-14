#pragma once

#include "pagemanager.h"
#include <optional>
#include <memory>

namespace db {
namespace disk {

	enum state {
    BT_OVERFLOW,
    BT_UNDERFLOW,
    OKK,
  };

template <class T, int BTREE_ORDER = 3> class btree {
public:
  struct node {
    long page_id{-1};
		long next_id{-1};
    long count{0};
    
    T data[BTREE_ORDER + 1];
    long children[BTREE_ORDER + 2];
		
    node(long page_id) : page_id{page_id} {
      count = 0;
      for (int i = 0; i < BTREE_ORDER + 2; i++) {
        children[i] = 0;
      }
    }

    // insert value in data and children by index
    void insert_in_node(int index, const T &value) {
      int j = count;

      while (j > index) {
        data[j] = data[j - 1];
        children[j + 1] = children[j];
        j--;
      }

      data[j] = value;
      children[j + 1] = children[j];
			
			count++;
		}


		void erase_in_node (int index) {
			int j = count;
			
			while (j > index) {
				data[j - 1] = data [j];
				children[j - 1] = children[j];
				j --;
			}
			
			children[j - 1] = children[j];
			count--;
		}

		bool is_overflow() { return count > BTREE_ORDER; }
		bool is_underflow() { return count < (BTREE_ORDER / 2); } 
  };

  struct Metadata {
    long root_id{1};
    long count{0};
  } header;


private:
  std::shared_ptr<pagemanager> pm;

public:
	class iterator : std::iterator<
										  std::forward_iterator_tag,
											T,
											T,
											T*,
											T& > {
			std::optional<node> root;
			std::optional<node> ending;
			int pos	= 0;
			int pos_end	= 0;
			btree<T, BTREE_ORDER>* instance;
		public:
			iterator(btree* instance) : instance(instance){}
			iterator(btree* instance, node start){
				root = std::make_optional(start);
			}
			iterator(btree* instance, node start, int count) : instance(instance) {
				root = std::make_optional(start);
				pos = count;
			}
			iterator(btree* instance, node start, int count, node end, int count_end) : instance(instance){
				root = std::make_optional(start);
				ending = std::make_optional(end);
				pos = count;
				pos_end = count_end;
			}
			iterator& operator++(){
				// if root does exists/null
				if (!root) {
					return *this;
				}
				
				// if pos does not overflow root
				if (root.value().count - 1 > pos) {
					pos ++;
					return *this;
				}

				// if pos++ is at the end node
				if (ending && ending.value().data[pos_end] == root.value().data[pos]) {
					root = std::nullopt;
					return *this;
				}

				// id pos++ is in the next node
				root = std::make_optional(instance->read_node(root.value().next_id));
				return *this;
			}
			iterator operator++(int){
				iterator it = *this;
				++(*this);
				return *this;
			}
			

			bool operator==(iterator& it) const{
				if (!root && !it.root) return true;

				if (!root || !it.root) return false;

				if (root.value().data[pos] == it.root.value().data[pos]) {
					return true;
				}
			}
			

			bool operator!=(iterator& it) const{
				return !(*this == it);
			}
			

			T operator*(){
				return root.value().data[pos];
			}
	};
	btree(std::shared_ptr<pagemanager> pm) : pm{pm} {
    if (pm->is_empty()) {
      node root{header.root_id};
      pm->save(root.page_id, root);

      header.count++;

      pm->save(0, header);
    } else {
      pm->recover(0, header);
    }
  }

 node new_node () {
    header.count++;
    node ret {header.count};
    pm->save (0, header);
    return ret;
  }

  node read_node (long page_id) {
    node n{-1};
    pm->recover(page_id, n);
    return n;
  }

  void write_node (long page_id, node n) { pm->save (page_id, n); }
  void delete_node (long page_id, node n) { pm->erase (page_id, n); }

  void insert(const T &value) {
    node root = read_node(header.root_id);
    int state = insert(root, value);

    if (state == BT_OVERFLOW) {
      split_root();
    }
  }

  int insert (node &ptr, const T &value) {
      int index = 0;
    
		while (index < ptr.count && ptr.data[index] < value) {
      index++;
		}

    if (ptr.children[index] != 0) {
      long page_id = ptr.children[index];
      node child = read_node(page_id);
      int state = insert(child, value);
      if (state == BT_OVERFLOW) {
        split(ptr, index);
      }
    } else {
      ptr.insert_in_node(index, value);
      write_node(ptr.page_id, ptr);
    }
    return ptr.is_overflow() ? BT_OVERFLOW : OKK;
	}

	void erase (const T &value) {
		node root = read_node (header.root_id);
		int state = erase (root, value);

		if (state == BT_UNDERFLOW) {
			std::cout << "merge_root\n";
			merge_root ();
		}
	}

	int erase (node &ptr, const T &value) {
		int index = 0;

		while (index < ptr.count &&  ptr.data[index] <= value) {
			if (ptr.data[index] == value) {
				std::cout << "index-> " << index << '\n';
				ptr.erase_in_node (index);
			}
			index ++;
		}
	
		if (ptr.children[index] != 0) { // go down a level if its not a leaf
			long page_id = ptr.children[index];
			node child = read_node (page_id);
			int state = erase (child, value);

			if (state == BT_UNDERFLOW) {
				merge (ptr, index);
			}
		} else {
			ptr.erase_in_node (index);
			write_node (ptr.page_id, ptr);
		}

		return ptr.is_underflow() ? BT_UNDERFLOW : OKK;
	}


  void split(node &parent, int pos) {
    node ptr = this->read_node(parent.children[pos]);
    node left = this->new_node();
    node right = this->new_node();

    int iter = 0;
    int i;
    for (i = 0; iter < BTREE_ORDER / 2; i++) {
      left.children[i] = ptr.children[iter];
      left.data[i] = ptr.data[iter];
      left.count++;
      iter++;
    }
		
    left.children[i] = ptr.children[iter];
		int j = i - 1;
		
    parent.insert_in_node(pos, ptr.data[iter]);
		if (ptr.children[iter] == 0) { // if is leaf node
			iter++; // the middle element
			for (i = 0; iter < BTREE_ORDER + 1; i++) {
				right.children[i] = ptr.children[iter];
				right.data[i] = ptr.data[iter - 1];
				right.data[i + 1] = ptr.data[iter];
				right.count += 2;

				iter++;
			}	
		} else {
			iter++; // the middle element
			for (i = 0; iter < BTREE_ORDER + 1; i++) {
				right.children[i] = ptr.children[iter];
				right.data[i] = ptr.data[iter];
				right.count ++;
				iter++;
			}
		}
		left.next_id = right.page_id; 

    right.children[i] = ptr.children[iter];

    parent.children[pos] = left.page_id;
    parent.children[pos + 1] = right.page_id;

    write_node(parent.page_id, parent);
    write_node(left.page_id, left);
    write_node(right.page_id, right);
  } 
 

	void merge (node &parent, int pos) {
    node ptr = this->read_node(parent.children[pos + 1]);
		node root_ptr = this->read_node (this->header.root_id);
		node left = this->new_node();
    node right = this->new_node();

    int iter = 0;
    int i;
		
		if (!parent.data[pos - 1]) { //if leaf left is empty
			for (i = 0; iter < BTREE_ORDER / 2; i++) {
				left.children[i] = ptr.children[iter];
				left.data[i] = ptr.data[iter];
				left.count++;
				iter++;
			}

			left.children[i] = ptr.children[iter];

			for (i = 0; iter < BTREE_ORDER; i++) {
				right.children[i] = ptr.children[iter];
				right.data[i] = ptr.data[iter];
				right.count ++;
				iter++;
			}
		
			left.next_id = right.page_id; 

			right.children[i] = ptr.children[iter];

			parent.children[pos] = left.page_id;
			parent.children[pos + 1] = right.page_id;
			parent.data[0] = right.data[right.count - 1];

			write_node(parent.page_id, parent);
			write_node(left.page_id, left);
			write_node(right.page_id, right);
		}
	
  }

  void split_root() {
    node ptr = this->read_node(this->header.root_id);
    node left = this->new_node();
    node right = this->new_node();

    int pos = 0;
    int iter = 0;
    int i;
    for (i = 0; iter < BTREE_ORDER / 2; i++) {
      left.children[i] = ptr.children[iter];
      left.data[i] = ptr.data[iter];
      left.count++;
      iter++;
    }

    left.children[i] = ptr.children[iter];
		
		if (ptr.children[iter] == 0) {
			iter ++; // the middle element
			for (i = 0; iter < BTREE_ORDER + 1; i++) {
				right.children[i] = ptr.children[iter];
				right.data[i] = ptr.data[iter - 1];
				right.data[i + 1] = ptr.data[iter];
				right.count += 2;
				iter++;
			}
		} else {
			iter++; // the middle element
			for (i = 0; iter < BTREE_ORDER + 1; i++) {
				right.children[i] = ptr.children[iter];
				right.data[i] = ptr.data[iter];
				right.count++;
				iter++;
			}
		}

    right.children[i] = ptr.children[iter];

    ptr.children[pos] = left.page_id;
    ptr.data[0] = ptr.data[BTREE_ORDER / 2];
    ptr.children[pos + 1] = right.page_id;
    ptr.count = 1;
		
		if (ptr.children[iter] == 0) {
			left.next_id = right.page_id;
		}

    write_node(ptr.page_id, ptr);
    write_node(left.page_id, left);
    write_node(right.page_id, right);
  }


	void merge_root() {
    node ptr = this->read_node(this->header.root_id);
    node left = this->new_node();
    node right = this->new_node();

    int pos = 0;
    int iter = 0;
    int i;
    
		if (!ptr.children[0]) {

		for (i = 0; iter < BTREE_ORDER / 2; i++) {
      left.children[i] = ptr.children[iter];
      left.data[i] = ptr.data[iter];
      left.count++;
      iter++;
    }

    left.children[i] = ptr.children[iter];
		
		if (ptr.children[iter] == 0) {
			iter ++; // the middle element
			for (i = 0; iter < BTREE_ORDER + 1; i++) {
				right.children[i] = ptr.children[iter];
				right.data[i] = ptr.data[iter - 1];
				right.data[i + 1] = ptr.data[iter];
				right.count += 2;
				iter++;
			}
		} else {
			iter++; // the middle element
			for (i = 0; iter < BTREE_ORDER + 1; i++) {
				right.children[i] = ptr.children[iter];
				right.data[i] = ptr.data[iter];
				right.count++;
				iter++;
			}
		}

    right.children[i] = ptr.children[iter];

    ptr.children[pos] = left.page_id;
    ptr.data[0] = ptr.data[BTREE_ORDER / 2];
    ptr.children[pos + 1] = right.page_id;
    ptr.count = 1;
		
		if (ptr.children[iter] == 0) {
			left.next_id = right.page_id;
		}

    write_node(ptr.page_id, ptr);
    write_node(left.page_id, left);
    write_node(right.page_id, right);
		}
  }

 
	typename btree<T, BTREE_ORDER>::iterator end () {
		return iterator(this);
	}
	
	iterator begin() {
		node root = read_node (header.root_id);
		return iterator(this, root);
	}

	iterator find (const T &value) { 
		node root = read_node(header.root_id);
		return find(root, 0, value);
	}

	iterator find (node &ptr, int level, const T &value) {
		int i;
		
    for (i = ptr.count - 1; i >= 0; i--) {
      if (ptr.children[i + 1]) {
        node child = read_node (ptr.children [i + 1]);

				if (ptr.data[i] > value) {
					child = read_node (ptr.children [i - 1]);
					find (child, level + 1, value);
				}
				
				else {
					find(child, level + 1, value);
				}
      }

			if (ptr.data[i] > value) {
				return iterator (this);
			}

			if (ptr.data[i] < value) {
				return iterator (this);
			}

			if (ptr.data[i] == value) {
				return iterator (this, ptr);
			}

    }
    
		if (ptr.children[i + 1]) {
      node child = read_node(ptr.children[i + 1]);
      
			find(child, level + 1, value);
    }
  }

	void print(std::ostream& out) {
    node root = read_node(header.root_id);
    print(root, 0, out);
  }

  void print(node &ptr, int level, std::ostream& out) {
    int i;
    for (i = 0; i < ptr.count; i++) {
      if (ptr.children[i]) {
        node child = read_node(ptr.children[i]);
        print(child, level + 1, out);
      } 
			else {
				out	<< ptr.data[i];
			}
    }
    if (ptr.children[i]) {
      node child = read_node(ptr.children[i]);
      print(child, level + 1, out);
    }
  }


  void print_tree() {
    node root = read_node(header.root_id);
    print_tree(root, 0);
    std::cout << "________________________\n";
  }

  void print_tree(node &ptr, int level) {
    int i;
    for (i = ptr.count - 1; i >= 0; i--) {
      if (ptr.children[i + 1]) {
        node child = read_node(ptr.children[i + 1]);
        print_tree(child, level + 1);
      }

      for (int k = 0; k < level; k++) {
        std::cout << "    ";
      }
      std::cout << ptr.data[i] << "\n";
    }
    if (ptr.children[i + 1]) {
      node child = read_node(ptr.children[i + 1]);
      print_tree(child, level + 1);
    }
  }
};
} // namespace disk
} // namespace db

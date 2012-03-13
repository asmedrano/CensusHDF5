from tables import *

import ri_geographies
import seq_num_table_lookup


h5file = None

def main():

	h5file = openFile("HDF5/census.h5", mode = "w", title = "Census Data")
	ri_geographies.load(h5file)
	seq_num_table_lookup.load(h5file)
	h5file.close()



if __name__ == "__main__":
	main()

import os 
import sys
import math


BLOCK_SIZE = 100   #100 tuples in each block 



def get_info_on_files(r_file_path, s_file_path) :

	num_r_records = 0
	num_s_records = 0

	with open(r_file_path, "r") as f:
		for line in f:
			num_r_records += 1

	with open(s_file_path, "r") as f:
		for line in f:
			num_s_records += 1


	num_r_blocks = math.ceil(num_r_blocks/100)
	num_s_blocks = math.ceil(num_s_blocks/100)

	res = {}

	res["r"] = {"blocks" : num_r_blocks, 
				"records" : num_r_records}
	res["s"] = {"blocks" : num_s_blocks, 
				"records" : num_s_records}

	return res
				



def create_sorted_sublists(R) :


def phaseOne(ram_size,input_file,output_file,cols_to_sort,sorting_order,input_file_size,isThreading):
	record_size = getMetaData()
	total_records = 0
	cols_to_sort_indices = []

	#read input file to count no of records in the file
	try :
		with open(input_file,'r') as inp:
			while True :
				data = inp.readline()
				if(data.rstrip()) :
					total_records += 1
				else :
					break
	except (OSError,IOError) as e:
		print(e)
		sys.exit(0)

	#get the column indices which need to be sorted :
	for i in cols_to_sort: 
		cols_to_sort_indices.append(metadata[i][1])
	
	#calculate number of records in each file 
	ram_size = ram_size * FACTOR 
	print("ram size : ",ram_size)
	print("Record size : " ,record_size)
	num_records_in_subfile = ram_size//(record_size+4)
	print("subfile record : ",num_records_in_subfile)
	print("total_record : ", total_records)

	#calculate no of subfiles 
	num_of_subfiles = math.ceil(total_records/num_records_in_subfile)
	if(input_file_size < ram_size) :
		print("file size is less than main memory size -- two phase merge sort not required")
	print("Number of subfiles :" , num_of_subfiles)

	print("cols to sort are : ", cols_to_sort)


	###SPLITTING THE MAIN FILE AND SORTING EACH SUBFILE
	with open(input_file) as inp :
		#data = inp.readlines()
		print("read file in alist")
		temp = 0	   
		subfile = 1    #stores the current subfile no. that is being sorted
		for i in range(0,num_of_subfiles) :
			''''sublist = []   #stores the records for each file before getting written into subfile
			#splitting the main file
			for j in range(temp, temp + num_records_in_subfile) :
				#print(j)
				if(j == len(data)) :
					print("inside subfile")
					break
					
				#PROCESS  A ROW OF INPUT FILE : 
				row_data = processOneRow(data[j])
				sublist.append(row_data)
			temp += num_records_in_subfile'''
			#print(i)
			sublist = []
			for j in range(num_records_in_subfile) :
				#print("inside secind ine")
				temp_data = inp.readline().rstrip()#.strip('\n')
				#print(temp_data)
				if(temp_data == '') :
					break
				sublist.append(processOneRow(temp_data))
				#print("temp_data")


				#sorting the data
			print(f"###sorting subfile{i+1}")
			if(sorting_order == 'asc') :
				sublist = sorted(sublist,key=itemgetter(*cols_to_sort_indices))
					#sublist = sorted(sublist,key=itemgetter(0))
			else :
				sublist = sorted(sublist,key=itemgetter(*cols_to_sort_indices), reverse=True)
			file_name = 'subfile' + str(subfile) + '.txt'

			#writing the sublist to subfile
			with open(file_name,'w') as f:
				for i in sublist:
					for j in i :
						f.write(str(j))
						f.write('  ')
					f.write('\n')
			subfile += 1


def open(r_file_path, s_file_path, num_blocks) :
	output_file = open(r_file_path)
	




def sort_merge_join(r_file_path, s_file_path, num_blocks) :

	#Create sorted sublists for R and S, each of size M blocks.
	open(r_file_path,s_file_path,num_blocks)


def main(args) :

	r_file_path = args[0]
	s_file_path = args[1]
	join_type = args[2]
	num_blocks = args[3]

	#exit if file doesn't exist
	if(not os.path.exists(r_file_path)) :
		print("File containing R doesn't exist")
		sys.exit(0)

	if(not os.path.exists(s_file_path)) :
		print("File containing S doesn't exist")
		sys.exit(0)

	get_info_on_files(r_file_path,s_file_path)

	if(join_type == "sort") :
		sort_merge_join(r_file_path, s_file_path, num_blocks)

	else :
		hash_join(r_file_path, s_file_path, num_blocks)




if __name__ == "__main__" :

	if(len(sys.argv) < 5) :
		print("Please provide correct input")
		sys.exit()

	main(sys.argv)

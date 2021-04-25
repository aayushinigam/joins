import os 
import sys
import math
import heapq
from operator import itemgetter
import time


BLOCK_SIZE = 100   #100 tuples in each block 


class heapNode:

	def __init__(self,cols_to_sort,row,file_name):
		self.cols_to_sort = cols_to_sort
		self.row = row
		self.file_name = file_name


	def __lt__(self,other) :

		#for ascending order
		if(self.row[self.cols_to_sort] < other.row[self.cols_to_sort]) :
				return True
		else:
				return False
		return False




def get_info_on_files(r_file_path, s_file_path) :

	num_r_records = 0
	num_s_records = 0

	with open(r_file_path, "r") as f:
		for line in f:
			num_r_records += 1

	with open(s_file_path, "r") as f:
		for line in f:
			num_s_records += 1

	
	num_r_blocks = math.ceil(num_r_records/BLOCK_SIZE)
	num_s_blocks = math.ceil(num_s_records/BLOCK_SIZE)

	print("\nNo. of records in R = ", num_r_records)
	print("No. of blocks in R = ", num_r_blocks)
	print("No. of records in S = ", num_s_records)
	print("No. of blocks in S = ", num_s_blocks)

	res = {}

	res["R"] = {"records" : num_r_records,
				"blocks" : num_r_blocks 
			   }

	res["S"] = {"records" : num_s_records,
				"blocks" : num_s_blocks}

	output_file = os.path.basename(r_file_path)+'_' + os.path.basename(s_file_path)+'_join.txt'

	return res, output_file
				



def create_sorted_sublists(input_file,col_to_sort,record_dict,mm_blocks) :

	if(col_to_sort) :
		relation = "R"
	else :
		relation = "S"
	
	###SPLITTING THE MAIN FILE AND SORTING EACH SUBFILE
	with open(input_file) as inp :
		sublist = []
		subfile_num = 1
		for row in inp :
			temp_data = row.rstrip()

			#break when file ends : 
			if(temp_data == '') :
				break
			#append data to temporary sublist 
			sublist.append(temp_data.split())


			#when main memory limit reaches, sort the sublist and write it to a subfile
			if(sys.getsizeof(sublist) > (BLOCK_SIZE * mm_blocks)) :
				file_name = 'subfile' + relation +str(subfile_num) + '.txt'
				sublist = sorted(sublist,key=itemgetter(col_to_sort))

				#writing the sublist to subfile
				with open(file_name,'w') as f:
					for i in sublist:
						for j in i :
							f.write(str(j))
							f.write(' ')
						f.write('\n')

				sublist = []
				subfile_num += 1


		#handle last sublist 
		if(sublist) :
			sublist = sorted(sublist,key=itemgetter(col_to_sort))
			file_name = 'subfile' + relation + str(subfile_num) + '.txt'

			#writing the sublist to subfile
			with open(file_name,'w') as f:
				for i in sublist:
					for j in i :
						f.write(str(j))
						f.write(' ')
					f.write('\n')

		sorted_file_name = "Sorted" + relation + ".txt"
		sort_relation_entirely(subfile_num,sorted_file_name,relation,col_to_sort)
		split_sorted_relation(subfile_num,sorted_file_name,relation,mm_blocks, record_dict)
		delete_file(sorted_file_name)


def delete_file(file) :
	if(os.path.exists(file)) :
		os.remove(file)


def split_sorted_relation(num_of_subfiles,file,relation,mm_blocks,record_dict) :

	blocks = record_dict["blocks"]

	###SPLITTING THE MAIN FILE AND SORTING EACH SUBFILE
	with open(file) as inp :
		sublist = []
		subfile_num = 1
		count = 0
		for i in range(blocks) :
			file_name = 'sorted_subfile' + relation +str(i+1) + '.txt'
			with open(file_name,"w") as out : 
				for i in range(BLOCK_SIZE) :
					temp_data = inp.readline().rstrip()
					if(temp_data == "") :
						return
					count += 1
					out.write(temp_data + "\n")



def sort_relation_entirely(num_of_subfiles,output_file,relation,col_to_sort) : 
	outfile = open(output_file,"w")
	subfile_pointers = {}  #list of file pointers
	relation_heap = []	
	file_end_flag = False	


	#build intial heap with first line of every file
	for i in range(0,num_of_subfiles) :
		file_name = 'subfile' + relation + str(i+1) + '.txt'
		subfile = open(file_name,"r")
		subfile_pointers[file_name] = subfile
		subfile_data = subfile.readline()

		#PROCESS  A ROW OF INPUT FILE : 
		row_data = subfile_data.rstrip().split()
		node = heapNode(col_to_sort,row_data,file_name)
		relation_heap.append(node)


	#node = heapq.heappop(relation_heap)
	heapq.heapify(relation_heap)

	while(len(relation_heap) > 0) :

		#get root data from heap
		node = heapq.heappop(relation_heap)

		#write data to output file
		for i in node.row :
			outfile.write(str(i) + " ")
		outfile.write("\n")

		#get the next line 
		current_file = subfile_pointers[node.file_name]
		temp_row_data = current_file.readline()

		#if the line is not empty 
		if(temp_row_data) :
			next_row_data = temp_row_data.rstrip().split()
			temp_node = heapNode(col_to_sort, next_row_data, node.file_name)
			relation_heap.append(temp_node)
			heapq.heapify(relation_heap)

		#file has reached its end
		else :
			temp_file_name = node.file_name
			os.remove(temp_file_name)
			del subfile_pointers[temp_file_name]

	outfile.close()



def open_sort(r_file_path, s_file_path, records_dict, mm_blocks) :
	
	#sort relation R
	print("\n###Sorting Relation R\n")
	create_sorted_sublists(r_file_path,1,records_dict["R"], mm_blocks)
	#sort relation S
	print("\n###Sorting Relation S\n")
	create_sorted_sublists(s_file_path,0,records_dict["S"], mm_blocks)

	

def get_next_sort(relation, file_pointer, records_dict)	:

	global curr_block_R
	global curr_block_S
	global lines_read_R
	global lines_read_S

	#initialise variables based on relation
	if relation == 'R':
		temp_file_name = "RelationR.txt"
		block_file_name = "sorted_subfileR"
		num_blocks = records_dict["R"]["blocks"]
		y_col = 1
		lines_read = lines_read_R
		curr_block = curr_block_R

	else :
		temp_file_name = "RelationS.txt"
		block_file_name = "sorted_subfileS"
		num_blocks = records_dict["S"]["blocks"]
		y_col = 0
		lines_read = lines_read_S
		curr_block = curr_block_S


	file_pointer.seek(0,0)
	for i in range(lines_read) :
		next(file_pointer)

	temp_record = file_pointer.readline()

	#print(relation, curr_block)

	#open the file to store temporary results 
	temp_relation_res = open(temp_file_name, 'w')
	
	#print("Record is : ", temp_record)

	#if file ends : 
	if not temp_record:
		return False, file_pointer

	#else keep storing values
	y_val = (temp_record.rstrip().split())[y_col]
	temp_relation_res.write(temp_record)
	lines_read += 1
	while(1):
		temp_record = file_pointer.readline()
		if temp_record :
			temp_val = (temp_record.rstrip().split())[y_col]
			if(temp_val == y_val) :
				temp_relation_res.write(temp_record)
				lines_read += 1
			else :
				temp_relation_res.close()
				if(relation == "R") :
					lines_read_R = lines_read
				else :
					lines_read_S = lines_read
				return True, file_pointer
		else :
			curr_block += 1
			#update current pointer values of global variables - 
			if(relation == "R") :
				curr_block_R = curr_block
				lines_read_R = lines_read
			else :
				curr_block_S = curr_block
				lines_read_S = lines_read

			if curr_block > num_blocks:
				return True, file_pointer

			else:
				file_pointer.close()
				file_pointer = open(block_file_name + str(curr_block) +'.txt', 'r')
				lines_read = 0
		



                             
def check_memory(records_dict,mm_blocks) :
	num_r_blocks = records_dict["R"]["blocks"]
	num_s_blocks = records_dict["S"]["blocks"]
	if((num_r_blocks + num_s_blocks) > (mm_blocks ** 2)) :
		return False
	else :
		return True



def sort_merge_join(r_file_path, s_file_path, records_dict, mm_blocks, output_file) :

	#Create sorted sublists for R and S, each of size M blocks.
	open_sort(r_file_path, s_file_path, records_dict, mm_blocks)

	if(not check_memory(records_dict,mm_blocks)) :
		print("Not enough memory")
		return 
	
	#initialise variables for joining step : 
	global curr_block_R 
	global curr_block_S
	global lines_read_R
	global lines_read_S
	
	curr_block_R = 1 
	curr_block_S = 1
	lines_read_R = 0
	lines_read_S = 0	 

	file_pointer_R = open("sorted_subfileR1.txt", "r")
	file_pointer_S = open("sorted_subfileS1.txt", "r")
	output_file_pointer = open(output_file, "w")
	
	print("Beginning to perform join....\n")
	#initialise get_next : 
	flag, file_pointer_R = get_next_sort("R", file_pointer_R, records_dict)
	flag, file_pointer_S = get_next_sort("S", file_pointer_S, records_dict)


	#join step : 
	while flag :


		with open("RelationR.txt", "r") as R_file :
			R_record = R_file.readline()
			R_val = (R_record.rstrip().split(' '))[1]
		with open("RelationS.txt", "r") as S_file :
			S_record = S_file.readline()
			S_val = (S_record.rstrip().split(' '))[0]


		#if the S val is less, get next from S -
		if (R_val > S_val) :
			flag, file_pointer_S = get_next_sort('S',file_pointer_S, records_dict)

		#if R val is less, get next from R - 
		elif(R_val < S_val) : 
			flag, file_pointer_R = get_next_sort('R', file_pointer_R, records_dict)

		#if they are equal, write in result file : 
		else :
			write_output(output_file_pointer)
			flag, file_pointer_R = get_next_sort('R', file_pointer_R, records_dict)
			flag, file_pointer_S = get_next_sort('S', file_pointer_S, records_dict)

	
	
	close_sort(file_pointer_R,file_pointer_S,records_dict)
	print("\nJoin done\n")



def close_sort(R_pointer, S_pointer, records_dict) :
	R_pointer.close()
	S_pointer.close()
	delete_file('RelationR.txt')
	delete_file('RelationS.txt')
	delete_file("SortedR.txt")
	delete_file("SortedS.txt")
	for i in range(records_dict["R"]["blocks"]) :
		delete_file("sorted_subfileS" + str(i+1) + ".txt")
	for i in range(records_dict["S"]["blocks"]) :
		delete_file("sorted_subfileR" + str(i+1) + ".txt")


def write_output(file_pointer) :
	with open("RelationR.txt", "r") as R_file : 
		R_record = R_file.readline()
		while R_record:
			with open("RelationS.txt", "r") as S_file :
				S_record = S_file.readline()
				while S_record:
					temp_R = R_record.rstrip().split()
					temp_S = S_record.rstrip().split()
					x = temp_R[0]
					y = temp_R[1]
					z = temp_S[1]
					file_pointer.write(x+ " " + y+ " " + z +"\n")
					S_record = S_file.readline()
			R_record = R_file.readline()
	delete_file("RelationR.txt")
	delete_file("RelationS.txt")
	#return file_pointer



def hash_open(r_file_path, s_file_path, mm_blocks) :
	pass

def get_next_hash() :
	pass

def hash_close() :
	pass

def hash_join(r_file_path,s_file_path,records_dict,mm_blocks) :
	pass




def main(args) :

	r_file_path = args[1]  #path to file containing R
	s_file_path = args[2]  #path to file containing S
	join_type = args[3]
	mm_blocks = int(args[4])  		   #no. of blocks available 

	#exit if file doesn't exist
	if(not os.path.exists(r_file_path)) :
		print("File containing R doesn't exist")
		sys.exit(0)

	if(not os.path.exists(s_file_path)) :
		print("File containing S doesn't exist")
		sys.exit(0)

	#get file metadata
	records_dict, output_file = get_info_on_files(r_file_path,s_file_path)
	begin = time.time()
	if(join_type == "sort") :
		sort_merge_join(r_file_path, s_file_path, records_dict, mm_blocks, output_file)

	else :
		hash_join(r_file_path, s_file_path, records_dict, mm_blocks, output_file)

	end = time.time()

	print(f"TIME TAKEN : {end-begin}")



if __name__ == "__main__" :

	if(len(sys.argv) < 5) :
		print("Please provide correct input")
		sys.exit()

	main(sys.argv)

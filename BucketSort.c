#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <mpi.h>
#include <assert.h>
#define SIZE 10000
#define MAX_PROCESSES 100

typedef unsigned long ULONG;

//compare function used for qsort
int cmpfunc (const void * a, const void * b) {
   return ( *(int*)a - *(int*)b );
}

int main() {

	srand(12345);

	int i, j, k;
 	
	MPI_Status Stat;
    MPI_Init(NULL, NULL);

    // world_rank is process id
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    // world_size is total processes
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    //in this case, first divide the original large array into several partitions with equal items
    //the number of partitions (or large bukets) is equal to the processor numbers used for parallel computing
    const int nums_per_proc = SIZE / world_size;
    const int Divide = (int)ceil(RAND_MAX / world_size);
    //malloc memeory of large Buckets on all the processors
    int * bucketBuffer = (int*)malloc(nums_per_proc * sizeof(int));
    assert(bucketBuffer != NULL);
   	//the original long array
   	int *longArr = NULL;
   	//the displacement of the long array
    int *displs = NULL;
    //the final number of items in each processor
    int *final_num_per_proc = (int*)malloc(world_size * sizeof(int));
    assert(final_num_per_proc != NULL);
    //the long array is initialized on root processor
    if(world_rank == 0) {
    	
		longArr = (int*)malloc(SIZE * sizeof(int));
		assert(longArr != NULL);
	
		printf("the original arrary is:\n");
		//initial long array with random numbers
		for(int i=0; i<SIZE; i++) {
			longArr[i] = rand();
			printf("%d ", longArr[i]);
		}
		printf("\n\n");
	}
	//scatter the large buckets from root to all processors
	MPI_Scatter(longArr, nums_per_proc, MPI_INT, bucketBuffer, 
		nums_per_proc, MPI_INT, 0, MPI_COMM_WORLD);

	//the bucket is a array for storing all the small buckets
	int *bucket = (int*)malloc(nums_per_proc * sizeof(int));
	assert(bucket != NULL);
	memset(bucket, 0, nums_per_proc * sizeof(int));
	//calculate the numbers of integers in each small buckets on each processor
	//notice that each small buckets will have different numbers of intergers because all the integers are randomly generated
	int * nums_per_small_bucket = (int*)malloc(world_size * sizeof(int));
	assert(nums_per_small_bucket != NULL);
	memset(nums_per_small_bucket, 0, world_size * sizeof(int));
	for(i=0, j=0; i<nums_per_proc; i++) {
		j = bucketBuffer[i] / Divide;
		nums_per_small_bucket[j]++;
	}
	//split represent the start point of each small buckets in the bucket
	int *split = (int*)malloc(world_size * sizeof(int));
	assert(split != NULL);
	memset(split, 0, world_size * sizeof(int));

	for(j=1; j<world_size; j++) {
		split[j] = split[j-1] + nums_per_small_bucket[j-1];
	}

	//put the integers in large bucket (bucketBuffer) into bucket;
	//notice that in bucket the integers are ordered between small bucket, although the number in small bucket is unordered
	//all the integers in sb0 (aka, small bucket 0) is smaller than sb1, and integers in sb1 are smaller than those in sb2 and so on  
	int *index = (int*)malloc(world_size * sizeof(int));
	assert(index != NULL);
	memset(index, 0, world_size * sizeof(int));
	for(i=0; i<nums_per_proc; i++) {
		j = bucketBuffer[i] / Divide;
		k = index[j];
		bucket[split[j]+k] = bucketBuffer[i];
		index[j]++;
	}

	//before Alltoallv action, we need an Alltoall action in order to send the numbers of each small buckets to other processors
	//otherwise, other processors will not know the exact memory size to allocate
	int * recv_count = (int*)malloc(world_size * sizeof(int));
	assert(recv_count != NULL);
	memset(recv_count, 0, world_size * sizeof(int));

	MPI_Alltoall(nums_per_small_bucket, 1, MPI_INT, recv_count, 1, MPI_INT, MPI_COMM_WORLD);

	// initial the offset of each small bucket in the received large bucket
	// notices that all the intergers received from all processors (include itself) will be stored in one large bucket
	int * recv_offset = (int*)malloc(world_size * sizeof(int));
	assert(recv_offset != NULL);
	memset(recv_offset, 0, world_size * sizeof(int));
	//calculate the total numbers receive from other proceesors
	//this variable will used for allocate memories to receive all the integers sent by other processors
	int totalnums_received = recv_count[0];

	for(int i=1; i<world_size; i++) {
		recv_offset[i] = recv_offset[i-1] + recv_count[i-1];
		totalnums_received += recv_count[i];
	}
	//the final large bucket on each proccessor
	bucketBuffer = (int*)realloc(bucketBuffer, totalnums_received * sizeof(int));

	//Alltoallv action to sent all small buckets to all processors (include the send processor itself)
	//after the action, all the integers on processor 0 is smaller than processor 1, processor 1 smaller than process 2 and so on.
	MPI_Alltoallv(&bucket[0], &nums_per_small_bucket[0], &split[0], MPI_INT, 
		bucketBuffer, &recv_count[0], &recv_offset[0], MPI_INT, MPI_COMM_WORLD);
	//quick sort on each processor
	qsort(bucketBuffer, totalnums_received, sizeof(int), cmpfunc);
	//first gather the total numbers of large buckets on each processor
	MPI_Gather(&totalnums_received, 1, MPI_INT, 
	 	final_num_per_proc, 1, MPI_INT, 0, MPI_COMM_WORLD);
	//calcalate the displacement of each large buckets when insert in the long array
	if(world_rank == 0) {
		displs = (int*)malloc(world_size * sizeof(int));
		memset(displs, 0, world_size * sizeof(int));
		for(int i=1; i<world_size; i++) {
			displs[i] = displs[i-1] + final_num_per_proc[i-1];
		}
	}
	//now gather all the sorted numbers in each large buckets and insert them in the correct displacement
	MPI_Gatherv(bucketBuffer, totalnums_received, MPI_INT, 
		longArr, final_num_per_proc, displs, MPI_INT, 0, MPI_COMM_WORLD);

	//check the result on root processor
	if(world_rank == 0) {
		printf("the sorted longArr is:\n");
		for(int i=0; i<SIZE; i++) {
			printf("%d\n", longArr[i]);
		}
		printf("\n\n");
	}

	//free all the dynamic space
	if(world_rank == 0) {
		free(longArr);
		free(displs);
	}

	free(bucketBuffer);
	free(final_num_per_proc);
	free(bucket);
	free(split);
	free(recv_offset);

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
	
	return 0;

}

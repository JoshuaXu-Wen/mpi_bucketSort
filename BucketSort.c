#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <mpi.h>
#include <assert.h>
#define SIZE 10000
#define MAX_PROCESSES 100

typedef unsigned long ULONG;

int cmpfunc (const void * a, const void * b) {
   return ( *(int*)a - *(int*)b );
}

// int * bucketInitialize( int *arr,  int length) {
// 	int *newarr = (int*)malloc(length * sizeof(int));



// }

int main() {

	srand(12345);

	int i, j, k;
 	//int counts[MAX_PROCESSES] = {0};
 	
	MPI_Status Stat;
    MPI_Init(NULL, NULL);

    // world_rank is process id
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    // world_size is total processes
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);


    const int nums_per_proc = SIZE / world_size;
    const int Divide = (int)ceil(RAND_MAX / world_size);
    //initial large Buckets on all the processors
    int * bucketBuffer = (int*)malloc(nums_per_proc * sizeof(int));
    assert(bucketBuffer != NULL);
   	//initial the original long array
   	int *longArr = NULL;
   	//the displacement of the array
    int *displs = NULL;
    int *final_num_per_proc = (int*)malloc(world_size * sizeof(int));
    assert(final_num_per_proc != NULL);

    if(world_rank == 0) {
    	
    	//int counters = world_size;
    	
		longArr = (int*)malloc(SIZE * sizeof(int));
		assert(longArr != NULL);
		//displs = (int*)malloc(sizeof(int) * world_size);
		//assert(displs != NULL);

		// int j = 0;
		// //displs[0] = 0;

		// for(int i=0; i<SIZE; i++) {
		// 	longArr[i] = rand();
		// 	j = longArr[i] / Divide;
		// 	counts[j]++;
		// }
		// printf("displs[0] is: %d\n", displs[0]);
		// for(j=1; j<world_size; j++) {
		// 	displs[j] = counts[j-1] + displs[j-1];
		// 	printf("displs[%d] is: %d\n", j, displs[j]);
		// }
		printf("the original arrary is:\n");
		for(int i=0; i<SIZE; i++) {
			longArr[i] = rand();
			printf("%d ", longArr[i]);
			//j = longArr[i] / Divide;
			//counts[j]++;
		}

		printf("\n\n");

		// printf("the original counts is:\n");

		// for(j=0; j<world_size; j++) {
		// 	printf("%d \n", counts[j] );
		// }	
		// printf("\n\n");


	}
	MPI_Scatter(longArr, nums_per_proc, MPI_INT, bucketBuffer, 
		nums_per_proc, MPI_INT, 0, MPI_COMM_WORLD);


	// MPI_Scatter(longArr, 1, MPI_INT, &nums_per_proc, 
	//  	1, MPI_INT, 0, MPI_COMM_WORLD);

	//printf("the nums on process %d is: %d\n", world_rank, nums_per_proc);
		//caculate the nums that will sent to each buckets



	//if(world_rank == 0) {
	// MPI_Scatterv(longArr, counts, displs, MPI_INT, bucketBuffer, 
	// 		nums_per_proc, MPI_INT, 0, MPI_COMM_WORLD);
	//}
	//initial the small bucket
	//int *bucket[world_size] = {NULL};
	int *bucket = (int*)malloc(nums_per_proc * sizeof(int));
	assert(bucket != NULL);
	memset(bucket, 0, nums_per_proc * sizeof(int));
	//define the numbers in small buckets 
	//int nums_per_small_bucket[world_size] = {0};
	int * nums_per_small_bucket = (int*)malloc(world_size * sizeof(int));
	assert(nums_per_small_bucket != NULL);
	memset(nums_per_small_bucket, 0, world_size * sizeof(int));

	//int index_per_small_bucket[world_size] = {0};  
	//int recv_count[world_size] = {0};
	int * recv_count = (int*)malloc(world_size * sizeof(int));
	assert(recv_count != NULL);
	memset(recv_count, 0, world_size * sizeof(int));

	for(i=0, j=0; i<nums_per_proc; i++) {
		j = bucketBuffer[i] / Divide;
		nums_per_small_bucket[j]++;

	}

	int *split = (int*)malloc(world_size * sizeof(int));
	assert(split != NULL);
	memset(split, 0, world_size * sizeof(int));

	printf("the total numbers in each small bucket on process 0 is: %d\n", nums_per_small_bucket[0]);
	for(j=1; j<world_size; j++) {
		split[j] = split[j-1] + nums_per_small_bucket[j-1];
		printf("the total numbers in each small bucket on process %d is: %d\n", world_rank, nums_per_small_bucket[j]);
	}

	int *index = (int*)malloc(world_size * sizeof(int));
	assert(index != NULL);
	memset(index, 0, world_size * sizeof(int));
	for(i=0; i<nums_per_proc; i++) {
		j = bucketBuffer[i] / Divide;
		k = index[j];
		bucket[split[j]+k] = bucketBuffer[i];
		index[j]++;
	}

	// for (j=0; j<world_size; j++) {
	// 	bucket[j] = (int *) calloc(nums_per_small_bucket[j], sizeof(int));
	// }

	// //scatter items from large buckets into small buckets
	// for(i=0; i<nums_per_proc; i++) {
	// 	j = bucketBuffer[i] / Divide;
	// 	k = index[j];
	// 	bucket[j][k] = bucketBuffer[i];
	// 	index[j]++;
	// }

	//free(bucketBuffer);
	//bucketBuffer = (int*)malloc(bucket[world_rank])
	MPI_Alltoall(nums_per_small_bucket, 1, MPI_INT, recv_count, 1, MPI_INT, MPI_COMM_WORLD);
	// initial the total numbers in the received large bucket
	//int send_offset[world_size] = {0};
	//int recv_offset[world_size] = {0};
	int * recv_offset = (int*)malloc(world_size * sizeof(int));
	assert(recv_offset != NULL);
	memset(recv_offset, 0, world_size * sizeof(int));

	int totalnums_received = recv_count[0];
	for(int i=1; i<world_size; i++) {
		recv_offset[i] = recv_offset[i-1] + recv_count[i-1];
		//send_offset[i] = send_offset[i-1] + nums_per_small_bucket[i-1];
		totalnums_received += recv_count[i];
	}
	//the final large bucket on each proccessor
	bucketBuffer = (int*)realloc(bucketBuffer, totalnums_received * sizeof(int));


	MPI_Alltoallv(&bucket[0], &nums_per_small_bucket[0], &split[0], MPI_INT, 
		bucketBuffer, &recv_count[0], &recv_offset[0], MPI_INT, MPI_COMM_WORLD);

	qsort(bucketBuffer, totalnums_received, sizeof(int), cmpfunc);

	MPI_Gather(&totalnums_received, 1, MPI_INT, 
	 	final_num_per_proc, 1, MPI_INT, 0, MPI_COMM_WORLD);

	if(world_rank == 0) {
		displs = (int*)malloc(world_size * sizeof(int));
		memset(displs, 0, world_size * sizeof(int));
		for(int i=1; i<world_size; i++) {
			displs[i] = displs[i-1] + final_num_per_proc[i-1];
		}
	}

	MPI_Gatherv(bucketBuffer, totalnums_received, MPI_INT, 
		longArr, final_num_per_proc, displs, MPI_INT, 0, MPI_COMM_WORLD);


	if(world_rank == 0) {
		printf("the sorted longArr is:\n");
		for(int i=0; i<SIZE; i++) {
			printf("%d\n", longArr[i]);
		}
		printf("\n\n");
	}

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

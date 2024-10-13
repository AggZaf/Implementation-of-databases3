#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include "hp_file.h"
#include "record.h"
#include "sort.h"
#include "merge.h"
#include "chunk.h"

int compareRecords(const void* a, const void* b);
bool shouldSwap(Record* rec1,Record* rec2){
    return false;
}

void sort_FileInChunks(int file_desc, int numBlocksInChunk){
    CHUNK_Iterator* tsankiter;
    tsankiter = malloc(sizeof(CHUNK_Iterator));
    *tsankiter = CHUNK_CreateIterator(file_desc,numBlocksInChunk);
    CHUNK* tsank;
    tsank = malloc(sizeof(CHUNK));

    int block_count;    
    BF_GetBlockCounter(file_desc, &block_count);
    block_count--;
    
    int numChunks = (block_count + numBlocksInChunk - 1) / numBlocksInChunk; // Calculate total number of chunks
    printf("NUMCHUNKS=%d\n",numChunks);
    for (int x=0; x< numChunks; x++){
        CHUNK_GetNext(tsankiter,tsank);
        sort_Chunk(tsank);
        CHUNK_Print(*tsank);
    }

    free(tsankiter);
    free(tsank);

}

// Function to swap two records
void swapRecords(Record *a, Record *b) {
    Record temp = *a;
    *a = *b;
    *b = temp;
}

void sort_Chunk(CHUNK* chunk) {
    int totalRecords = chunk->recordsInChunk;
    Record* allRecords = malloc(totalRecords * sizeof(Record));

    int currentRecord = 0;
    for (int i = chunk->from_BlockId; i <= chunk->to_BlockId; i++) {
        int numRecordsInBlock = HP_GetRecordCounter(chunk->file_desc, i);

        for (int j = 0; j < numRecordsInBlock; j++) {
            Record* rec = malloc(sizeof(Record));
            HP_GetRecord(chunk->file_desc, i, j, rec);
            memcpy(&allRecords[currentRecord], rec, sizeof(Record));
            free(rec);
            currentRecord++;
            HP_Unpin(chunk->file_desc,i);
        }
    }

    qsort(allRecords, totalRecords, sizeof(Record), compareRecords);

    currentRecord = 0;
    for (int i = chunk->from_BlockId; i <= chunk->to_BlockId; i++) {
        int numRecordsInBlock = HP_GetRecordCounter(chunk->file_desc, i);

        for (int j = 0; j < numRecordsInBlock; j++) {
            HP_UpdateRecord(chunk->file_desc, i, j, allRecords[currentRecord]);
            currentRecord++;
            HP_Unpin(chunk->file_desc,i);
        }
    }

    free(allRecords);

}

// Comparison function for sorting records based on name and surname
int compareRecords(const void* a, const void* b) {
    Record* recordA = (Record*)a;
    Record* recordB = (Record*)b;
    
    // Compare names first
    int nameCompare = strcmp(recordA->name, recordB->name);
    
    // If names are the same, compare surnames
    if (nameCompare == 0) {
        return strcmp(recordA->surname, recordB->surname);
    }
    
    return nameCompare;
}


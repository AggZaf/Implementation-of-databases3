#include <merge.h>
#include <stdio.h>
#include <stdbool.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>




/* Merge function for merging bWay chunks into a single chunk. */
void merge(int input_FileDesc, int chunkSize, int bWay, int output_FileDesc) {
    CHUNK_Iterator* iterators[bWay];
    CHUNK* chunks[bWay];

    for (int i = 0; i < bWay; i++) {
        // Use the concatenated name to create the iterator
        iterators[i] = malloc(sizeof(CHUNK_Iterator));
        *iterators[i] = CHUNK_CreateIterator(input_FileDesc, chunkSize);
        iterators[i]->current = iterators[i]->current + (chunkSize * i); // Set the current value
        
        chunks[i] = malloc(sizeof(CHUNK));
        chunks[i]->blocksInChunk = chunkSize;
        chunks[i]->file_desc = input_FileDesc;
        chunks[i]->from_BlockId = iterators[i]->current;
        chunks[i]->to_BlockId = chunks[i]->from_BlockId + chunkSize -1;
        int total_recs =0;
        for (int x=0 ; x < iterators[i]->blocksInChunk ; x++){
                total_recs = total_recs + HP_GetRecordCounter(iterators[i]->file_desc, iterators[i]->current+x);
        }
        chunks[i]->recordsInChunk = total_recs;

    }

    printf("META TO PROTO FOR\n");
    Record* records[bWay];
    
    for (int y=0; y<bWay; y++){
        records[y] = malloc(sizeof(Record));
        HP_GetRecord(input_FileDesc,iterators[y]->current, 0,records[y]);
        HP_Unpin(input_FileDesc,iterators[y]->current);
    }

    for(int z=0; z<bWay; z++){
        printRecord(*records[z]);
    }
    
    //Record* records[bWay];
    qsort(records, bWay, sizeof(Record),compareRecords);

    for(int z=0; z<bWay; z++){
        printRecord(*records[z]);
    }
    
}

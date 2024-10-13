#include <merge.h>
#include <stdio.h>
#include "chunk.h"


CHUNK_Iterator CHUNK_CreateIterator(int fileDesc, int blocksInChunk){
    // Function to create a CHUNK_Iterator
    CHUNK_Iterator iterator;
    int blocks_num;
    // Initialize the iterator with provided values
    iterator.file_desc = fileDesc;
    iterator.current = 1; // Start from the beginning-- current block
    BF_GetBlockCounter(fileDesc,&blocks_num); blocks_num--; //lastblockid
    iterator.lastBlocksID = blocks_num; //to id tou teleutaiou block tou chunk
    if (blocks_num < blocksInChunk){        //se periptosi pou arxika to arxeio exei ligotera blocks apo osa exume orisei gia ena chunk
        //iterator.lastBlocksID = blocks_num;
        iterator.blocksInChunk = blocks_num; 
    }
    else{                                   //diaforetika ola ok
        //iterator.lastBlocksID = blocksInChunk;
        iterator.blocksInChunk = blocksInChunk;
    }
    return iterator;
}


int CHUNK_GetNext(CHUNK_Iterator *iterator, CHUNK *chunk) {
        // Check if the iterator has reached the end of chunks
    if (iterator->current > iterator->lastBlocksID) {
        printf("No More Chunks to Access\n");
        return -1; // Indicates no more chunks to retrieve
    }

    chunk->file_desc = iterator->file_desc;

    // Calculate the remaining blocks after the current position
    int remainingBlocks = iterator->lastBlocksID - iterator->current + 1;

    // Determine the number of blocks in this chunk
    chunk->blocksInChunk = (remainingBlocks < iterator->blocksInChunk) ? remainingBlocks : iterator->blocksInChunk;

    // Set the block range for the chunk
    chunk->from_BlockId = iterator->current;
    chunk->to_BlockId = iterator->current + chunk->blocksInChunk - 1;

    // Move iterator to the next chunk
    iterator->current += chunk->blocksInChunk;

    // Calculate records in the chunk
    int total_recs = 0;
    for (int i = chunk->from_BlockId; i <= chunk->to_BlockId ; i++) {
        total_recs += HP_GetRecordCounter(iterator->file_desc, i);
    }
    chunk->recordsInChunk = total_recs;
    return 0; 
    
    /*
    // Check if the iterator has reached the end of chunks
    if (iterator->current >= iterator->lastBlocksID) {
        printf("No More Chunks to Access\n");
        return -1; // Indicates no more chunks to retrieve
    }

    chunk->file_desc = iterator->file_desc;
    // Calculate the remaining blocks after the current position
    int remainingBlocks = iterator->lastBlocksID - iterator->current + 1;
    if (iterator->current + iterator->blocksInChunk > iterator->lastBlocksID){
        return -1;
    }
    iterator->current = iterator->current + iterator->blocksInChunk;
    if ((iterator->lastBlocksID - iterator->current) < iterator->blocksInChunk){
        iterator->blocksInChunk = (iterator->lastBlocksID - iterator->current);
    }
    int total_recs = 0;
    int LastBlockChunk = iterator->current+iterator->blocksInChunk;
    for (int i = iterator->current; i <= LastBlockChunk; i++) {
        total_recs += HP_GetRecordCounter(iterator->file_desc, i);
    }

    chunk->blocksInChunk = iterator->blocksInChunk;
    chunk->from_BlockId = iterator->current;
    chunk->to_BlockId = LastBlockChunk;
    chunk->recordsInChunk = total_recs;


    return 0; // Indicates successful retrieval of a chunk
    */
}


int CHUNK_GetIthRecordInChunk(CHUNK* chunk,  int i, Record* record){

    if (chunk == NULL || i < 1 || i >= chunk->recordsInChunk) {
        printf("Record Id not Valid\n");
        return -1; // Μη επιτυχής ανάκτηση
    }

    int blockId = (chunk->from_BlockId + i / HP_GetMaxRecordsInBlock(chunk->file_desc));
    int cursor = (i % HP_GetMaxRecordsInBlock(chunk->file_desc))-1;

    if (HP_GetRecord(chunk->file_desc, blockId, cursor, record) == -1) {
        return -1; // Μη επιτυχής ανάκτηση
    }
    printf("The %dth record in the chunk is:\n",i);
    printRecord(*record);
    printf("And is stored in the slot %d, of the %dth block.\n",cursor,blockId);
    return 0; // Επιτυχής ανάκτηση
}






int CHUNK_UpdateIthRecord(CHUNK* chunk, int i, Record record) {

    if (chunk == NULL || i < 1 || i >= chunk->recordsInChunk) {
        printf("Record Id not Valid\n");
        return -1; // Μη επιτυχής ανάκτηση
    }

    int blockId = (chunk->from_BlockId + i / HP_GetMaxRecordsInBlock(chunk->file_desc));
    int cursor = (i % HP_GetMaxRecordsInBlock(chunk->file_desc))-1;

    if (HP_UpdateRecord(chunk->file_desc, blockId, cursor, record) == -1) {
        return -1; // Μη επιτυχής ανάκτηση
    }


    printf("The %dth record has been updated with the following record:\n",i);
    printRecord(record);

    return 0; // Success
}

void CHUNK_Print(CHUNK chunk) {
    printf("Chunk starts from block:%d\n",chunk.from_BlockId);
    printf("Chunk finishes to block:%d\n",chunk.to_BlockId);
    printf("Chunks file descriptor :%d\n",chunk.file_desc);
    printf("Blocks In Chunk        :%d\n",chunk.blocksInChunk);
    printf("Records In Chunk       :%d\n",chunk.recordsInChunk);
    for (int blockId = chunk.from_BlockId; blockId <= chunk.to_BlockId; ++blockId) {
        printf("Block %d:\n", blockId);
        int numRecords = HP_GetRecordCounter(chunk.file_desc, blockId);

        for (int recordId = 0; recordId < numRecords; ++recordId) {
            Record rec;
            HP_GetRecord(chunk.file_desc, blockId, recordId, &rec);
            printf("Record %d: RecordID=%d, Name=%s, Surname=%s\n", recordId, rec.id, rec.name, rec.surname);
        }
        printf("\n");
    }
}

CHUNK_RecordIterator CHUNK_CreateRecordIterator(CHUNK* chunk){
/* Function to create a record iterator for efficient traversal within a CHUNK */

    CHUNK_RecordIterator iterator;
    /* Initialize the fields of the iterator with provided values */
    iterator.chunk = *chunk; /* Assuming a copy is needed, change this based on requirements */
    iterator.currentBlockId = 1; /* Start with the first block */
    iterator.cursor = 0; /* position of iterator inside the block*/
    return iterator;
}

/* Function to get the next record from the CHUNK */
int CHUNK_GetNextRecord(CHUNK_RecordIterator *iterator, Record *record) {
    CHUNK *chunkPtr = &(iterator->chunk);
    int file_descriptor = chunkPtr->file_desc;/* File descriptor for the heap file */

    /* Increment cursor */
    iterator->cursor++;
    int max_recs_in_block = HP_GetMaxRecordsInBlock(file_descriptor);
    /* Check if the current block and cursor are within the boundaries */
    if (iterator->currentBlockId > chunkPtr->to_BlockId ||          //an vgo ektos tou teleutaiou block tou chunk
        (iterator->currentBlockId == chunkPtr->to_BlockId && iterator->cursor > max_recs_in_block-1)) {  //i an eimai sto teleutaio block alla vgo ektos tis teleutaias thesis
        /* Reached the end of records, return 0 to indicate no more records */
        printf("No more records in this chunk!\n");
        iterator->cursor--;
        return 1;
    }
    else{
        if (iterator->cursor > max_recs_in_block-1){ //an eimai stin teleutaia thesi enos block, na pao sto epomeno block
            iterator->currentBlockId++;
            iterator->cursor=0;           
        }
    }

    /* Use HP_GetRecord to retrieve the record */
    HP_GetRecord(file_descriptor, iterator->currentBlockId, iterator->cursor, record);
    return 0;
}


int CHUNK_GetChunk(CHUNK_Iterator *iterator,CHUNK* chunk){
    chunk->blocksInChunk = iterator->blocksInChunk;
    chunk->file_desc = iterator->file_desc;
    chunk->from_BlockId = iterator->current;
    if (iterator->lastBlocksID < iterator->current+iterator->blocksInChunk){
        chunk->to_BlockId = iterator->lastBlocksID;
    }else{
        chunk->to_BlockId = iterator->current+iterator->blocksInChunk-1;
    }
    int total_recs =0;
    for (int i=0 ;i < iterator->blocksInChunk; i++){
        total_recs = total_recs + HP_GetRecordCounter(iterator->file_desc, iterator->current+i);
    }
    chunk->recordsInChunk = total_recs; 
    return 0;
}

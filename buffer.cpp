/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

FrameId frameNum;

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
  numBufs = bufs;
}

BufMgr::~BufMgr() {
	//flush out all dirty pages
	for (FrameId i = 0; i < numBufs; i++) {
        	if(bufDescTable[i].dirty) {
			flushFile(bufDescTable[i].file);
		}
	}
	//deallocate the buffer pool and the BufDesc table
	delete [] bufPool;
	delete [] hashTable;
	delete [] bufDescTable;
}

void BufMgr::advanceClock()
{
	//Moves the clock hand, wrapping around if necessary
	clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
	FrameIf current = 0;
	while(1) {
		advanceClock();

		if(bufDescTable[clockHand].valid)] {	

			//if pincount > 0, increment current
			if(bufDescTable[clockHand].pinCnt > 0) {
				current++;
				if(current == numBufs)
					throw BufferExceededException();
				continue;
			}

			//if referenced = 1, set to 0 and increment (advanceClock)
			else if(bufDescTable[clockhhand].refbit == 1) {
				bufDescTable[clockhhand].refbit = 0;
				continue;
			}

			//if referenced = 0 and pin count = 0, choose the page
			else if(bufDescTable[clockhhand].refbit == 0 && bufDescTable[clockHand].pinCnt == 0) {
				
				//if page is dirty, first we write it to the disk
				if(bufDescTable[clockHand].dirty == 1) {
					bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
				}

				hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
				bufDescTable[clockHand].Clear();
				frame = clockHand;
				break;
			}
		}
		else {
			//page is not valid, set BufDesc member variable values
			bufDescTable[clockHand].Set(bufDescTable[clockHand].file ,bufDescTable[clockHand].pageNo);
			frame = clockHand;
			break;
		}	
	}
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	//If file is in buffer pool, set reference to true and add a pin
	if(hashTable->lookup(file, pageNo, frameNum) {
		bufDescTable[frameNum].refbit = 1;
		bufDescTable[frameNum].pinCnt++;
		page = &bufPool[frameNum];
	}
	//If not, allocate it space and insert it
	else {
		allocBuf(frameNum);

		bufPool[frameNum] = file->readPage(frameNum);

		hashTable->insert(file,pageNo,frameNum);

		//Set the values
		bufDescTable[frameNum].Set(file, pageNo);

		page = &bufPool[frameNum];
	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	bool exceptionFlag = true;

	//if page is not found in hash table lookup, do nothing
	try {
		hashtable->lookup(file, pageNo, frameNum);
	}
	catch (HashNotFoundException e) {
		exceptionFlag = false;
	}

	//throw pageNotPinnedException if pin count is already 0
	if(exceptionFlag && bufDescTable[frameNum].pinCnt == 0) {
		throw PageNotPinnedException(bufDescTable[frameNum].file->filename(), bufDescTable[frameNum].pageNo, bufDescTable[frameNum].frameNo);
	}

	//decrement the pinCnt of the frame containing (file, PageNo) and, if dirty == true, sets the dirty bit 
	if(dirty && exceptionFlag) {
	        bufDescTable[frameNum].dirty = true;
	}

	if(exceptionFlag && bufDescTable[frameNum].pinCnt > 0) {
		bufDescTable[frameNum].pinCnt--;
	}
}

void BufMgr::flushFile(const File* file) 
{	
	//for each page in the buffer that belongs to the specified file we flush it to the disk if dirty, clear it, and remove it from hash table 
	for(FrameId i = 0; i < numBufs; i++) {
		if(bufDescTable[i].file == file)
		{
			if(bufDescTable[i].dirty == 1)
			{
				//page is dirty so write it to disk and update dirty bit
				bufDescTable[i].file->writePage(bufPool[i]);
				bufDescTable[i].dirty = 0;
			}

			//if page is pinned we don't want to flush it -> throw exception
			if(bufDescTable[i].pinCnt > 0)
				throw PagePinnedException("page is pinned", bufDescTable[i].pageNo, i);
	
			//if page is invalid throw exception
			if(bufDescTable[i].valid == 0)
				throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
	
			//remove from hash table and clear
			hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
			bufDescTable[i].Clear();
		}
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	//Create a new page, allocate into file
	Page pg = file->allocatePage();
	pageNo = pg.page_number();

	//Allocate space in the Buffer Pool for the new page
	allocBuf(frameNum);
	bufPool[frameNum] = pg;

	//set it's values
	bufDescTable[frameNum].Set(file,pageNo);

	//Also add it to the has table
	hashTable->insert(file,pageNo,frameNum);
	page = &bufPool[frameNum];

}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
     for(FrameId i = 0; i < numBufs; i++)
	{
		//loop through each file and page in the buffer and find the desired page
        	if(bufDescTable[i].file == file && bufDescTable[i].pageNo == PageNo)
        	{
			//if the page we want to remove is pinned, there is an error -> throw exception
			if(bufDescTable[i].pinCnt > 0)
				throw PagePinnedException("page is pinned", PageNo, i);
		
			//if it isn't pinned, then clear it from the buffer pool, remove it from the hash table and delete it
			bufDescTable[i].Clear();
			hashTable->remove(file,PageNo);    
		}
	}
	file->deletePage(PageNo);

}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	    tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}

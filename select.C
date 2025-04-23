/*
 * 
 * By Masa Abboud (mabboud@wisc.edu), Kavya Mathur (kmathur3@wisc.edu), Marissa Pederson (mapederson4@wisc.edu)
 *
 *  This file contains the implementation of the QU_Select and ScanSelect functions.
 *
 *
 */

#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"

// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);


/* 
 * Selects records from the specified relation.
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[], 
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
	// Convert attrInfo to AttrDesc
	AttrDesc projDescs[projCnt];
	for (int i = 0; i < projCnt; i++) {
    	Status status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projDescs[i]);
		if (status != OK) return status;
	}	

	// Prepare filter value
	const char* filter = nullptr;
	int intVal;
	float floatVal;

	if (attr != NULL) {
    	if (attr->attrType == INTEGER) {
        	intVal = *((int*) attr->attrValue);
        	filter = (char*)&intVal;
    	}
    	else if (attr->attrType == FLOAT) {
       		floatVal = *((float*) attr->attrValue);
        	filter = (char*)&floatVal;
    	}
   		else if (attr->attrType == STRING) {
        	filter = (char*) attr->attrValue;
    	}
	}

	// Compute output record length
	int reclen = 0;
	for (int i = 0; i < projCnt; i++) {
    	reclen += projDescs[i].attrLen;
	}

	// Get attrDesc if needed
	AttrDesc attrDesc;
	Status status;
	if (attr != NULL) {
		status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
		if (status != OK) return status;
	}

	status = ScanSelect(result, projCnt, projDescs, attr != NULL ? &attrDesc : NULL, op, filter, reclen);
	cout << "Doing QU_Select " << endl;

	return status;
}

/*
 * Scans a relation and selects records that match a given filter.
 * Projects specified attributes into a new result relation.
 * Returns:
 *  OK on success
 *  an error code otherwise
 */
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
	// Allocate buffer for result tuple
	char* tempData = new char[reclen];
	Record tempRec;
	tempRec.data = tempData;
	tempRec.length = reclen;

	// Open scan on source table
	Status status;
	HeapFileScan* hfs = new HeapFileScan(projNames[0].relName, status);
	if (status != OK) {
        delete hfs;
        delete[] tempData;
        return status;
    }

	// Open insert scan on result table
	Status status2;
	InsertFileScan* ins = new InsertFileScan(result, status2);
	if (status2 != OK) {
        delete hfs;
        delete ins;
        delete[] tempData;
        return status2;
    }

	// Conditional or unconditional scan
	if (attrDesc != NULL) {
		if (attrDesc->attrType == INTEGER) {
			int value = atoi(filter);
			status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, INTEGER, (char*)&value, op);
		} else if (attrDesc->attrType == FLOAT) {
			float value = atof(filter);
			status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, FLOAT, (char*)&value, op);
		} else if (attrDesc->attrType == STRING) {
			status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, STRING, filter, op);
		} else {
			status = BADFILE;
		}
	} else {
		status = hfs->startScan(0, 0, STRING, NULL, EQ);
	}

	if (status != OK) {
		delete hfs;
		delete ins;
		delete[] tempData;
		return status;
	}

	// Perform scan and insert into result table
	RID rid;
	Record rec;
	while ((status = hfs->scanNext(rid)) == OK) {
		status = hfs->getRecord(rec);
		if (status != OK) break;

		memset(tempData, 0, reclen);

		// Copy each projected attribute to correct offset in tempData
		int destOffset = 0;
		for (int i = 0; i < projCnt; i++) {
			memcpy(tempData + destOffset,
				   (char*)rec.data + projNames[i].attrOffset,
				   projNames[i].attrLen);
			destOffset += projNames[i].attrLen;
		}

		RID outrid;
		status = ins->insertRecord(tempRec, outrid);
		if (status != OK) break;
	}

	// Cleanup
	delete hfs;
	delete ins;
	delete[] tempData;

	return (status == FILEEOF) ? OK : status;
}

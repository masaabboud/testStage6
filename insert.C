/*
 *   By Masa Abboud (mabboud@wisc.edu), Kavya Mathur (Kmathur3@wisc.edu), Marissa Pederson (mapederson4@wisc.edu)
 *
 *  This file contains the implementation of the QU_Insert function.
 *  It inserts a new record into the specified relation
 *
 */

#include "catalog.h"
#include "query.h"
#include <cstring>
#include <cstdlib>

/*
  * Inserts a record into the specified relation.
  *
  * Returns:
  * 	OK on success
  * 	an error code otherwise
*/

const Status QU_Insert(const string &relation, 
                       const int attrCnt, 
                       const attrInfo attrList[]) 
{
    Status status = OK;

    RelDesc relDesc;
    status = relCat->getInfo(relation, relDesc);
    if (status != OK)
        return status;

    // get attribute info
    int relAttrCnt;
    AttrDesc* attrDescList;
    status = attrCat->getRelInfo(relation, relAttrCnt, attrDescList);
    if (status != OK)
        return status;

    int recordLength = 0;
    for (int i = 0; i < relAttrCnt; i++) {
        recordLength += attrDescList[i].attrLen;
    }

    // create new record and initialze
    Record record;
    record.length = recordLength;
    record.data = new char[record.length];
    if (!record.data) {
        delete[] attrDescList;
        return INSUFMEM;
    }
    memset(record.data, 0, record.length);

    // match up the values by their names
    for (int i = 0; i < relAttrCnt; i++) {
        for (int j = 0; j < attrCnt; j++) {
            if (strcmp(attrDescList[i].attrName, attrList[j].attrName) == 0) {
                char* dest = (char*)record.data + attrDescList[i].attrOffset;

                switch (attrDescList[i].attrType) {
                    case INTEGER: {
                        int val = atoi((char*)attrList[j].attrValue);
                        memcpy(dest, &val, sizeof(int));
                        break;
                    }
                    case FLOAT: {
                        float val = atof((char*)attrList[j].attrValue);
                        memcpy(dest, &val, sizeof(float));
                        break;
                    }
                    case STRING: {
                        strncpy(dest, (char*)attrList[j].attrValue, attrDescList[i].attrLen);
                        dest[attrDescList[i].attrLen - 1] = '\0';  // Ensure null termination
                        break;
                    }
                    default:
                        delete[] attrDescList;
                        delete[] (char*)record.data;
                        return UNIXERR;
                }
                break;
            }
        }
    }

    // insert
    InsertFileScan scan(relation, status);
    if (status != OK) {
        delete[] attrDescList;
        delete[] (char*)record.data;
        return status;
    }

    RID rid;
    status = scan.insertRecord(record, rid);

    delete[] attrDescList;
    delete[] (char*)record.data;
    return status;
}

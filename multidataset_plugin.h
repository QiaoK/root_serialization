#ifndef MULTIDATASET_PLUGIN_H
#define MULTIDATASET_PLUGIN_H

//#include <mpi.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <hdf5.h>
#include "H5Timing.h"
#define ENABLE_MULTIDATASET 0
#define MULTIDATASET_DEFINE 1
#undef PDC_PATCH

#ifdef PDC_PATCH
#include "pdc.h"
#endif

static int max_batch_size_g = 2;
static int hdf5_method_g = -1;
static int total_n_events_g = -1;


#if MULTIDATASET_DEFINE == 1
typedef struct H5D_rw_multi_t
{
    hid_t dset_id;          /* dataset ID */
    hid_t dset_space_id;    /* dataset selection dataspace ID */
    hid_t mem_type_id;      /* memory datatype ID */
    hid_t mem_space_id;     /* memory selection dataspace ID */
    union {
        void *rbuf;         /* pointer to read buffer */
        const void *wbuf;   /* pointer to write buffer */
    } u;
} H5D_rw_multi_t;
#endif

typedef struct multidataset_array {
#ifdef PDC_PATCH
    pdcid_t did;
    pdcid_t transfer_request_id;
    char* temp_mem;
#else
    std::vector<char*> *temp_mem;
    std::vector<hsize_t> *start;
    std::vector<hsize_t> *end;
    hsize_t last_end;
    hid_t did;
    hid_t mtype;      /* memory datatype ID */
#endif
} multidataset_array;

int set_hdf5_method(int hdf5_method);
int get_hdf5_method();
int init_multidataset();
int finalize_multidataset();
int register_multidataset_request_append(const char *name, hid_t gid, void *buf, hsize_t data_size, hid_t mtype);
int flush_multidatasets();
//int check_write_status();
#endif

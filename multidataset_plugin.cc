#include "multidataset_plugin.h"


static multidataset_array *multi_datasets;
static hid_t *dataset_recycle;
static hid_t *memspace_recycle;
static hid_t *dataspace_recycle;
static char** temp_mem;

static int dataset_size;
static int dataset_size_limit;
static int dataset_recycle_size;
static int dataset_recycle_size_limit;
static int dataspace_recycle_size;
static int dataspace_recycle_size_limit;
static int memspace_recycle_size;
static int memspace_recycle_size_limit;
hsize_t total_data_size;

#define MEM_SIZE 2048
#define MAX_DATASET 65536

int init_multidataset() {
    dataset_size = 0;
    dataset_size_limit = 0;
    dataspace_recycle_size = 0;
    dataspace_recycle_size_limit = 0;
    memspace_recycle_size = 0;
    memspace_recycle_size_limit = 0;
    dataset_recycle_size_limit = 0;
    dataset_recycle_size = 0;
    return 0;
}

int register_dataset_recycle(hid_t did) {
    if (dataset_recycle_size == dataset_recycle_size_limit) {
        if ( dataset_recycle_size_limit > 0 ) {
            dataset_recycle_size_limit *= 2;
            hid_t *temp = (hid_t*) malloc(dataset_recycle_size_limit*sizeof(hid_t));
            memcpy(temp, dataset_recycle, sizeof(hid_t) * dataset_recycle_size);
            free(dataset_recycle);
            dataset_recycle = temp;
        } else {
            dataset_recycle_size_limit = MEM_SIZE;
            dataset_recycle = (hid_t*) malloc(dataset_recycle_size_limit*sizeof(hid_t));
        }
    }
    dataset_recycle[dataset_recycle_size] = did;
    dataset_recycle_size++;
    return 0;
}


int register_dataspace_recycle(hid_t dsid) {
    if (dataspace_recycle_size == dataspace_recycle_size_limit) {
        if ( dataspace_recycle_size_limit > 0 ) {
            dataspace_recycle_size_limit *= 2;
            hid_t *temp = (hid_t*) malloc(dataspace_recycle_size_limit*sizeof(hid_t));
            memcpy(temp, dataspace_recycle, sizeof(hid_t) * dataspace_recycle_size);
            free(dataspace_recycle);
            dataspace_recycle = temp;
        } else {
            dataspace_recycle_size_limit = MEM_SIZE;
            dataspace_recycle = (hid_t*) malloc(dataspace_recycle_size_limit*sizeof(hid_t));
        }
    }
    dataspace_recycle[dataspace_recycle_size] = dsid;
    dataspace_recycle_size++;
    return 0;
}

int register_memspace_recycle(hid_t msid) {
    if (memspace_recycle_size == memspace_recycle_size_limit) {
        if ( memspace_recycle_size_limit > 0 ) {
            memspace_recycle_size_limit *= 2;
            hid_t *temp = (hid_t*) malloc(memspace_recycle_size_limit*sizeof(hid_t));
            memcpy(temp, memspace_recycle, sizeof(hid_t) * memspace_recycle_size);
            free(memspace_recycle);
            memspace_recycle = temp;
        } else {
            memspace_recycle_size_limit = MEM_SIZE;
            memspace_recycle = (hid_t*) malloc(memspace_recycle_size_limit*sizeof(hid_t));
        }
    }
    memspace_recycle[memspace_recycle_size] = msid;
    memspace_recycle_size++;
    return 0;
}

int register_multidataset(const char *name, void *buf, hid_t did, hid_t dsid, hid_t msid, hid_t mtype, int write) {
    int i;
    hsize_t dims[H5S_MAX_RANK];
    hsize_t mdims[H5S_MAX_RANK];
    hsize_t start[H5S_MAX_RANK];
    hsize_t end[H5S_MAX_RANK];
    char *tmp_buf;
    hsize_t data_size, total_data_size;
    hsize_t zero = 0;
    size_t esize = H5Tget_size (mtype);

    if (0&&write) {
        for ( i = 0; i < dataset_size; ++i ) {
            if (strcmp(name, multi_datasets[i].name) == 0) {
                /* Extract data size from input memory space */
                H5Sget_simple_extent_dims(msid, &data_size, mdims);
                /* Reset dataspace for existing dataset */
                H5Sget_simple_extent_dims(multi_datasets[i].dsid, dims, mdims);
                dims[0] += data_size;
                H5Sget_select_bounds(multi_datasets[i].dsid, start, end );
                H5Sset_extent_simple( multi_datasets[i].dsid, 1, dims, dims );
                /* Reset the end size to slab size*/
                end[0] = end[0] + 1 - start[0] + data_size;
                /* Add the new selection */
                H5Sselect_none(multi_datasets[i].dsid);
                H5Sselect_hyperslab(multi_datasets[i].dsid, H5S_SELECT_SET, start, NULL, end, NULL);
                H5Sclose(dsid);

                /* Reset the existing memory space directly to the current data size */
                H5Sget_simple_extent_dims(multi_datasets[i].msid, dims, mdims);
                dims[0] += data_size;
                total_data_size = dims[0];
                H5Sset_extent_simple( multi_datasets[i].msid, 1, dims, dims );
                H5Sselect_all( multi_datasets[i].msid );
                H5Sclose(msid);

                tmp_buf = temp_mem[dataset_size];
                temp_mem[dataset_size] = (char*) malloc(esize * total_data_size);
                memcpy(temp_mem[dataset_size], tmp_buf, esize * (total_data_size - data_size) );
                memcpy(temp_mem[dataset_size] + esize * (total_data_size - data_size), buf, esize * data_size );

                free(tmp_buf);
                multi_datasets[dataset_size].u.wbuf = temp_mem[dataset_size];

                H5Dclose(did);
                return 0;
            }
        }
    }

    H5Sget_simple_extent_dims (msid, dims, mdims);
    esize *= dims[0];

    if (dataset_size == dataset_size_limit) {
        if ( dataset_size_limit ) {
            dataset_size_limit *= 2;
            multidataset_array *temp = (multidataset_array*) malloc(dataset_size_limit*sizeof(multidataset_array));
            memcpy(temp, multi_datasets, sizeof(multidataset_array) * dataset_size);
            free(multi_datasets);
            multi_datasets = temp;

            char **new_memory = (char**) malloc(dataset_size_limit*sizeof(char*));
            memcpy(new_memory, temp_mem, sizeof(char*) * dataset_size);
            free(temp_mem);
            temp_mem = new_memory;
        } else {
            dataset_size_limit = MEM_SIZE;
            temp_mem = (char**) malloc(sizeof(char*) * dataset_size_limit);
            multi_datasets = (multidataset_array*) malloc(dataset_size_limit*sizeof(multidataset_array));
        }
    }

    multi_datasets[dataset_size].msid = msid;
    multi_datasets[dataset_size].did = did;
    multi_datasets[dataset_size].dsid = dsid;
    multi_datasets[dataset_size].mtype = mtype;
    strcpy(multi_datasets[dataset_size].name, name);
    if (write) {
        temp_mem[dataset_size] = (char*) malloc(esize);
        memcpy(temp_mem[dataset_size], buf, esize);
        multi_datasets[dataset_size].u.wbuf = temp_mem[dataset_size];
    } else {
        multi_datasets[dataset_size].u.rbuf = buf;
    }
    dataset_size++;

    register_dataset_recycle(did);
    register_dataspace_recycle(dsid);
    register_memspace_recycle(msid);
    return 0;
}

int check_write_status() {
    if (dataset_size < MAX_DATASET) {
        return 0;
    }
#ifdef H5_TIMING_ENABLE
    register_dataset_timer_start("flush_all");
#endif
    flush_multidatasets();
    dataset_recycle_all();
    dataspace_recycle_all();
    memspace_recycle_all();
#ifdef H5_TIMING_ENABLE
    register_dataset_timer_end(total_data_size);
#endif

    return 0;
}

int dataset_recycle_all() {
    int i;
    for ( i = 0; i < dataset_recycle_size; ++i ) {
        if ( dataset_recycle[i] >= 0 ) {
            H5Dclose(dataset_recycle[i]);
        }
    }
    if (dataset_recycle_size) {
        free(dataset_recycle);
    }
    dataset_recycle_size = 0;
    dataset_recycle_size_limit = 0;
    return 0;
}


int dataspace_recycle_all() {
    int i;
    //printf("recycle %d dataspace\n", dataspace_recycle_size);
    for ( i = 0; i < dataspace_recycle_size; ++i ) {
        if ( dataspace_recycle[i] >= 0 ) {
            H5Sclose(dataspace_recycle[i]);
        }
    }
    if (dataspace_recycle_size) {
        free(dataspace_recycle);
    }
    dataspace_recycle_size = 0;
    dataspace_recycle_size_limit = 0;
    return 0;
}

int memspace_recycle_all() {
    int i;
    //printf("recycle %d memspace\n", memspace_recycle_size);
    for ( i = 0; i < memspace_recycle_size; ++i ) {
        if ( memspace_recycle[i] >= 0 ){
            H5Sclose(memspace_recycle[i]);
        }
    }
    if (memspace_recycle_size) {
        free(memspace_recycle);
    }
    memspace_recycle_size = 0;
    memspace_recycle_size_limit = 0;
    return 0;
}

int flush_multidatasets() {
    int i;
    size_t esize;
    hsize_t dims[H5S_MAX_RANK], mdims[H5S_MAX_RANK];
    H5D_rw_multi_t *multi_datasets_temp;

    //printf("Rank %d number of datasets to be written %d\n", rank, dataset_size);
#if ENABLE_MULTIDATASET==1
    #ifdef H5_TIMING_ENABLE
    increment_H5Dwrite();
    #endif
    multi_datasets_temp = (H5D_rw_multi_t*) malloc(sizeof(H5D_rw_multi_t) * dataset_size);

    for ( i = 0; i < dataset_size; ++i ) {
        //MPI_Barrier(MPI_COMM_WORLD);
        #ifdef H5_TIMING_ENABLE
        increment_H5Dwrite();
        #endif
        multi_datasets_temp[i].dset_id = multi_datasets[i].did;
        multi_datasets_temp[i].dset_space_id = multi_datasets[i].dsid;
        multi_datasets_temp[i].mem_type_id = multi_datasets[i].mtype;
        multi_datasets_temp[i].mem_space_id = multi_datasets[i].msid;
        multi_datasets_temp[i].u.wbuf = multi_datasets[i].u.wbuf;
    }

    H5Dwrite_multi(H5P_DEFAULT, dataset_size, multi_datasets_temp);
    free(multi_datasets_temp);
#else

    //printf("rank %d has dataset_size %lld\n", rank, (long long int) dataset_size);
    for ( i = 0; i < dataset_size; ++i ) {
        //MPI_Barrier(MPI_COMM_WORLD);
        #ifdef H5_TIMING_ENABLE
        increment_H5Dwrite();
        #endif
        H5Dwrite (multi_datasets[i].did, multi_datasets[i].mtype, multi_datasets[i].msid, multi_datasets[i].dsid, H5P_DEFAULT, multi_datasets[i].u.wbuf);
    }
#endif

    for ( i = 0; i < dataset_size; ++i ) {
        H5Sget_simple_extent_dims (multi_datasets[i].msid, dims, mdims);
        esize = H5Tget_size (multi_datasets[i].mtype);
        total_data_size += dims[0] * esize;
    }

    //printf("rank %d number of hyperslab called %d\n", rank, hyperslab_count);
    for ( i = 0; i < dataset_size; ++i ) {
        free(temp_mem[i]);
    }
    if (dataset_size) {
        free(multi_datasets);
    }
    dataset_size = 0;
    dataset_size_limit = 0;
    return 0;
}

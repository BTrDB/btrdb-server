

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include "cephprovider.h"
#include <sys/time.h>

#define ADDR_LOCK_SIZE 0x1000000000
#define COMP_CAP_STEP 64

rados_t cluster;

void initialize_provider()
{
	int err;
	err = rados_create(&cluster, NULL);
	if (err < 0)
	{
		fprintf(stderr, "could not create RADOS cluster handle\n");
		errno = -err;
		return;
	}

	err = rados_conf_read_file(cluster, NULL);
	if (err < 0)
	{
		fprintf(stderr, "could not create load ceph conf\n");
		errno = -err;
		return;
	}

	err = rados_connect(cluster);
	if (err < 0)
	{
		fprintf(stderr, "could not create connect to cluster\n");
		errno = -err;
		return;
	}

	errno = 0;
}

cephprovider_handle_t* handle_create()
{
	int err;
	cephprovider_handle_t *rv = (cephprovider_handle_t*) malloc(sizeof(cephprovider_handle_t));
	rv->comps = (rados_completion_t*) malloc(sizeof(rados_completion_t) *COMP_CAP_STEP);
	rv->comp_cap = COMP_CAP_STEP;
	rv->comp_len = 0;

	err = rados_ioctx_create(cluster, "quasar", &rv->ctx);
	if (err < 0)
	{
		fprintf(stderr, "could not create io context\n");
		errno = -err;
		rados_ioctx_destroy(rv->ctx);
		free(rv);
		return NULL;
	}
	errno = 0;
	return rv;
}

void handle_write(cephprovider_handle_t *h, uint64_t address, const char* data, int len, int trunc)
{
	//The ceph provider uses 24 bits of address per object, and the top 40 bits as an object ID
	int offset = address & 0xFFFFFF;
	uint64_t id = address >> 24;
	int err;
	char oid [11];
	sprintf(oid, "%010lX", id);
	if (trunc)
	{
		err = rados_trunc(h->ctx, oid, len + offset);
		if (err < 0)
		{
			fprintf(stderr, "could not trunc\n");
			errno = -err;
			return;
		}
	}
	if (h->comp_len == h->comp_cap)
	{
		h->comp_cap += COMP_CAP_STEP;
		h->comps = realloc(h->comps, (h->comp_cap * sizeof(rados_completion_t)));
		if (!h->comps)
		{
			errno = -err;
			return;
		}
	}
	err = rados_aio_create_completion(NULL, NULL, NULL, &(h->comps[h->comp_len]));
	if (err < 0)
	{
		fprintf(stderr, "could not create completion\n");
		errno = -err;
		return;
	}
	err = rados_aio_write(h->ctx, oid, h->comps[h->comp_len], data, len, offset);
	if (err < 0)
	{
		fprintf(stderr, "could not aio write\n");
		errno = -err;
		return;
	}
	h->comp_len++;
	errno = 0;
}

int handle_read(cephprovider_handle_t *h, uint64_t address, char* dest, int len)
{
	//The ceph provider uses 24 bits of address per object, and the top 40 bits as an object ID
	int offset = address & 0xFFFFFF;
	uint64_t id = address >> 24;
	int rv;
	char oid [11];
	sprintf(oid, "%010lX", id);
	rv = rados_read(h->ctx, oid, dest, len, offset);
	if (rv < 0)
	{
		fprintf(stderr, "could not read \n");
		errno = -rv;
		return -1;
	}
	errno = 0;
	return rv;
}

void handle_init_allocator(cephprovider_handle_t *h)
{
	int err;
	struct timeval dur;
	dur.tv_sec = 5;
	dur.tv_usec = 0;
	uint64_t addr;
	if (h->comp_len == h->comp_cap)
	{
		h->comp_cap += COMP_CAP_STEP;
		h->comps = realloc(h->comps, (h->comp_cap * sizeof(rados_completion_t)));
		if (!h->comps)
		{
			errno = -err;
			return;
		}
	}
	err = rados_aio_create_completion(NULL, NULL, NULL, &(h->comps[h->comp_len]));
	if (err < 0)
	{
		fprintf(stderr, "could not create completion\n");
		errno = -err;
		return;
	}

	err = rados_lock_exclusive(h->ctx, "allocator", "alloc_lock", "main", "alloc", &dur, 0);
	if (err < 0) {
		fprintf(stderr, "could not lock allocator\n");
		errno = -err;
		return;
	}
	addr = 0x1000000; //Not zero!!
	err = rados_aio_write_full(h->ctx, "allocator", h->comps[h->comp_len], (char *) &addr, 8);
	if (err < 0) {
		fprintf(stderr, "could not write allocator\n");
		errno = -err;
		return;
	}
	rados_aio_wait_for_safe(h->comps[h->comp_len]);
	err = rados_unlock(h->ctx, "allocator", "alloc_lock", "main");
	if (err < 0) {
		fprintf(stderr, "could not unlock allocator\n");
		errno = -err;
		return;
	}
	rados_aio_release(h->comps[h->comp_len]);
	errno = 0;
}

//Returns the address of the start of a range that can be
//used
uint64_t handle_obtainrange(cephprovider_handle_t *h)
{
	int err;
	struct timeval dur;
	dur.tv_sec = 5;
	dur.tv_usec = 0;
	uint64_t addr;
	if (h->comp_len == h->comp_cap)
	{
		h->comp_cap += COMP_CAP_STEP;
		h->comps = realloc(h->comps, (h->comp_cap * sizeof(rados_completion_t)));
		if (!h->comps)
		{
			errno = -err;
			return 0;
		}
	}
	err = rados_aio_create_completion(NULL, NULL, NULL, &(h->comps[h->comp_len]));
	if (err < 0)
	{
		fprintf(stderr, "could not create completion\n");
		errno = -err;
		return 0;
	}
	err = rados_lock_exclusive(h->ctx, "allocator", "alloc_lock", "main", "alloc", &dur, 0);
	if (err < 0) {
		fprintf(stderr, "could not lock allocator\n");
		errno = -err;
		return 0;
	}
	err = rados_read(h->ctx, "allocator", (char *) &addr, 8, 0);
	if (err < 0) {
		fprintf(stderr, "could not read allocator\n");
		errno = -err;
		return 0;
	}
	printf("read allocation 0x%016lx\n",addr);
	addr += ADDR_LOCK_SIZE;
	printf("writing allocation 0x%016lx\n",addr);
	err = rados_aio_write_full(h->ctx, "allocator", h->comps[h->comp_len], (char *) &addr, 8);
	if (err < 0) {
		fprintf(stderr, "could not write allocator\n");
		errno = -err;
		return 0;
	}
	rados_aio_wait_for_safe(h->comps[h->comp_len]);
	err = rados_unlock(h->ctx, "allocator", "alloc_lock", "main");
	if (err < 0) {
		fprintf(stderr, "could not unlock allocator\n");
		errno = -err;
		return 0;
	}
	rados_aio_release(h->comps[h->comp_len]);
	errno = 0;
	printf("Returning %016lx\n", addr - ADDR_LOCK_SIZE);
	return addr - ADDR_LOCK_SIZE;
}

void handle_close(cephprovider_handle_t *h)
{
	int i;
	for (i=0; i < h->comp_len; i++)
	{
		rados_aio_wait_for_complete(h->comps[i]);
		rados_aio_release(h->comps[i]);
	}
	free(h->comps);
	rados_ioctx_destroy(h->ctx);
	free(h);

	errno = 0;
}

#ifdef STANDALONE

int main(int argc, char** argv)
{
	printf("Starting\n");
	initialize_provider();
	cephprovider_handle_t *h = handle_create();
	int i;
	for (i = 0; i< 100;i++) {
		char *data = malloc(16*1024*1024);
		//char data [6];
		handle_write(h, ((uint64_t)i)<<24, data,1600000, 1);
		//free(data);
	}
	printf("Finished AIO queue\n");
	handle_close(h);
	printf("Done writing\n");

	cephprovider_handle_t *h2 = handle_create();
	for (i = 0; i< 10;i++) {
		char *data = malloc(16*1024*1024);
		int rv;
		rv = handle_read(h2, ((uint64_t)i)<<24, data, 16*1024*1024-1);
		printf("rv was %d\n", rv);
	}
	printf("End\n");
}

#endif




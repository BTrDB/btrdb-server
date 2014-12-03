#include <stdint.h>
#include <rados/librados.h>

typedef struct
{
	rados_ioctx_t ctx;
	rados_completion_t *comps;
	int comp_len;
	int comp_cap;
} cephprovider_handle_t;

typedef cephprovider_handle_t* phandle_t;

void initialize_provider();
phandle_t handle_create();
void handle_write(phandle_t seg, uint64_t address, const char* data, int len, int trunc);
uint64_t handle_obtainrange(cephprovider_handle_t *h);
void handle_init_allocator(cephprovider_handle_t *h);
int handle_read(phandle_t seg, uint64_t address, char* dest, int len);
void handle_close(phandle_t seg);

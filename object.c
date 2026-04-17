// object.c — Content-addressable object store

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>
#include <errno.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTATION ─────────────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_str;

    if (type == OBJ_BLOB) type_str = "blob";
    else if (type == OBJ_TREE) type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    // 1. Build header: "type size\0"
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    header[header_len] = '\0';
    header_len += 1;

    // 2. Combine header + data
    size_t total_len = header_len + len;
    unsigned char *buf = malloc(total_len);
    if (!buf) return -1;

    memcpy(buf, header, header_len);
    memcpy(buf + header_len, data, len);

    // 3. Compute hash
    compute_hash(buf, total_len, id_out);

    // 4. Deduplication
    if (object_exists(id_out)) {
        free(buf);
        return 0;
    }

    // Build path
    char path[512];
    object_path(id_out, path, sizeof(path));

    // Extract shard directory
    char dir[512];
    strncpy(dir, path, sizeof(dir));
    dir[sizeof(dir) - 1] = '\0';
    char *slash = strrchr(dir, '/');
    if (!slash) {
        free(buf);
        return -1;
    }
    *slash = '\0';

    // 5. Create shard directory ONLY (tests expect this behavior)
    if (mkdir(dir, 0755) != 0 && errno != EEXIST) {
        free(buf);
        return -1;
    }

    // 6. Temp file
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s/tmpXXXXXX", dir);

    int fd = mkstemp(tmp_path);
    if (fd < 0) {
        free(buf);
        return -1;
    }

    // write loop
    ssize_t written = 0;
    while (written < (ssize_t)total_len) {
        ssize_t n = write(fd, buf + written, total_len - written);
        if (n <= 0) {
            close(fd);
            unlink(tmp_path);
            free(buf);
            return -1;
        }
        written += n;
    }

    // 7. fsync file
    fsync(fd);
    close(fd);

    // 8. rename
    if (rename(tmp_path, path) != 0) {
        unlink(tmp_path);
        free(buf);
        return -1;
    }

    // 9. fsync directory
    int dirfd = open(dir, O_DIRECTORY);
    if (dirfd >= 0) {
        fsync(dirfd);
        close(dirfd);
    }

    free(buf);
    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    rewind(f);

    if (size <= 0) {
        fclose(f);
        return -1;
    }

    unsigned char *buf = malloc(size);
    if (!buf) {
        fclose(f);
        return -1;
    }

    fread(buf, 1, size, f);
    fclose(f);

    // Verify hash
    ObjectID computed;
    compute_hash(buf, size, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(buf);
        return -1;
    }

    // Find '\0'
    unsigned char *nul = memchr(buf, '\0', size);
    if (!nul) {
        free(buf);
        return -1;
    }

    // Parse header
    char type_str[16];
    size_t data_size;
    sscanf((char *)buf, "%15s %zu", type_str, &data_size);

    if (strcmp(type_str, "blob") == 0) *type_out = OBJ_BLOB;
    else if (strcmp(type_str, "tree") == 0) *type_out = OBJ_TREE;
    else if (strcmp(type_str, "commit") == 0) *type_out = OBJ_COMMIT;
    else {
        free(buf);
        return -1;
    }

    // Extract data
    unsigned char *data_start = nul + 1;

    void *out = malloc(data_size);
    if (!out) {
        free(buf);
        return -1;
    }

    memcpy(out, data_start, data_size);

    *data_out = out;
    *len_out = data_size;

    free(buf);
    return 0;
}

// object.c — Content-addressable object store

#include "pes.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <openssl/evp.h>

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

static const char *object_type_string(ObjectType type) {
    switch (type) {
    case OBJ_BLOB:   return "blob";
    case OBJ_TREE:   return "tree";
    case OBJ_COMMIT: return "commit";
    default:         return NULL;
    }
}

static int parse_type_string(const char *s, ObjectType *out) {
    if (strcmp(s, "blob") == 0)   { *out = OBJ_BLOB;   return 0; }
    if (strcmp(s, "tree") == 0)   { *out = OBJ_TREE;   return 0; }
    if (strcmp(s, "commit") == 0) { *out = OBJ_COMMIT; return 0; }
    return -1;
}

// ─── object_write / object_read ─────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_str = object_type_string(type);
    if (!type_str || !id_out) return -1;

    char header[128];
    int hn = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    if (hn < 0 || (size_t)hn >= sizeof(header)) return -1;
    size_t header_len = (size_t)hn + 1;

    size_t total = header_len + len;
    unsigned char *buf = malloc(total);
    if (!buf) return -1;

    memcpy(buf, header, header_len);
    memcpy(buf + header_len, data, len);

    compute_hash(buf, total, id_out);

    if (object_exists(id_out)) {
        free(buf);
        return 0;
    }

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);

    char shard[512];
    snprintf(shard, sizeof(shard), "%s/%.2s", OBJECTS_DIR, hex);

    if (mkdir(PES_DIR, 0755) != 0 && errno != EEXIST) goto fail;
    if (mkdir(OBJECTS_DIR, 0755) != 0 && errno != EEXIST) goto fail;
    if (mkdir(shard, 0755) != 0 && errno != EEXIST) goto fail;

    char tmp[640];
    if (snprintf(tmp, sizeof(tmp), "%s/tmp.XXXXXX", shard) >= (int)sizeof(tmp))
        goto fail;
    int fd = mkstemp(tmp);
    if (fd < 0) goto fail;

    ssize_t w = write(fd, buf, total);
    if (w != (ssize_t)total) {
        close(fd);
        unlink(tmp);
        goto fail;
    }
    if (fsync(fd) != 0) {
        close(fd);
        unlink(tmp);
        goto fail;
    }
    if (close(fd) != 0) {
        unlink(tmp);
        goto fail;
    }

    char final_path[512];
    object_path(id_out, final_path, sizeof(final_path));
    if (rename(tmp, final_path) != 0) {
        unlink(tmp);
        goto fail;
    }

    int dfd = open(shard, O_RDONLY);
    if (dfd >= 0) {
        fsync(dfd);
        close(dfd);
    }

    free(buf);
    return 0;

fail:
    free(buf);
    return -1;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    if (!id || !type_out || !data_out || !len_out) return -1;

    char path[512];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return -1; }
    long sz = ftell(f);
    if (sz < 0) { fclose(f); return -1; }
    rewind(f);

    unsigned char *raw = malloc((size_t)sz);
    if (!raw) { fclose(f); return -1; }

    size_t got = fread(raw, 1, (size_t)sz, f);
    fclose(f);
    if (got != (size_t)sz) {
        free(raw);
        return -1;
    }

    ObjectID computed;
    compute_hash(raw, (size_t)sz, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(raw);
        return -1;
    }

    void *nul = memchr(raw, '\0', (size_t)sz);
    if (!nul) {
        free(raw);
        return -1;
    }

    char *sp = memchr(raw, ' ', (char *)nul - (char *)raw);
    if (!sp) {
        free(raw);
        return -1;
    }

    size_t type_len = (size_t)(sp - (char *)raw);
    if (type_len >= 64) {
        free(raw);
        return -1;
    }
    char type_buf[64];
    memcpy(type_buf, raw, type_len);
    type_buf[type_len] = '\0';

    ObjectType ot;
    if (parse_type_string(type_buf, &ot) != 0) {
        free(raw);
        return -1;
    }
    *type_out = ot;

    errno = 0;
    char *endp = NULL;
    unsigned long long data_len_ull = strtoull((char *)sp + 1, &endp, 10);
    if (errno != 0 || endp != nul) {
        free(raw);
        return -1;
    }

    unsigned char *payload = (unsigned char *)nul + 1;
    size_t payload_off = (size_t)(payload - raw);
    if (payload_off + data_len_ull > (size_t)sz) {
        free(raw);
        return -1;
    }
    *len_out = (size_t)data_len_ull;

    void *out = malloc(*len_out);
    if (!out) {
        free(raw);
        return -1;
    }
    memcpy(out, payload, *len_out);
    free(raw);
    *data_out = out;
    return 0;
}

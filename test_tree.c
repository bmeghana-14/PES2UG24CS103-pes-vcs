// test_tree.c — Phase 2 test program
//
// Compile and run:
//   make test_tree
//   ./test_tree

#include "pes.h"
#include "tree.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out);
void object_path(const ObjectID *id, char *path_out, size_t path_size);

void test_tree_roundtrip(void) {
    // Build a tree manually
    Tree original;
    original.count = 3;

    original.entries[0].mode = 0100644;
    memset(original.entries[0].hash.hash, 0xAA, HASH_SIZE);
    strcpy(original.entries[0].name, "README.md");

    original.entries[1].mode = 0040000;
    memset(original.entries[1].hash.hash, 0xBB, HASH_SIZE);
    strcpy(original.entries[1].name, "src");

    original.entries[2].mode = 0100755;
    memset(original.entries[2].hash.hash, 0xCC, HASH_SIZE);
    strcpy(original.entries[2].name, "build.sh");

    // Serialize
    void *data;
    size_t len;
    int rc = tree_serialize(&original, &data, &len);
    assert(rc == 0);
    assert(data != NULL);
    assert(len > 0);
    printf("Serialized tree: %zu bytes\n", len);

    // Parse back
    Tree parsed;
    rc = tree_parse(data, len, &parsed);
    assert(rc == 0);
    assert(parsed.count == 3);

    // Verify entries are sorted by name (tree_serialize must sort)
    assert(strcmp(parsed.entries[0].name, "README.md") == 0);
    assert(strcmp(parsed.entries[1].name, "build.sh") == 0);
    assert(strcmp(parsed.entries[2].name, "src") == 0);

    // Verify modes preserved
    assert(parsed.entries[0].mode == 0100644);
    assert(parsed.entries[1].mode == 0100755);
    assert(parsed.entries[2].mode == 0040000);

    // Verify hashes preserved
    assert(memcmp(parsed.entries[0].hash.hash, original.entries[0].hash.hash, HASH_SIZE) == 0);

    free(data);

    printf("PASS: tree serialize/parse roundtrip\n");
}

void test_tree_determinism(void) {
    // Same entries in different order must produce identical serialization
    Tree tree_a, tree_b;
    tree_a.count = 2;
    tree_b.count = 2;

    // tree_a: entries in order z, a
    tree_a.entries[0].mode = 0100644;
    memset(tree_a.entries[0].hash.hash, 0x11, HASH_SIZE);
    strcpy(tree_a.entries[0].name, "z_file.txt");
    tree_a.entries[1].mode = 0100644;
    memset(tree_a.entries[1].hash.hash, 0x22, HASH_SIZE);
    strcpy(tree_a.entries[1].name, "a_file.txt");

    // tree_b: entries in order a, z
    tree_b.entries[0].mode = 0100644;
    memset(tree_b.entries[0].hash.hash, 0x22, HASH_SIZE);
    strcpy(tree_b.entries[0].name, "a_file.txt");
    tree_b.entries[1].mode = 0100644;
    memset(tree_b.entries[1].hash.hash, 0x11, HASH_SIZE);
    strcpy(tree_b.entries[1].name, "z_file.txt");

    void *data_a, *data_b;
    size_t len_a, len_b;
    tree_serialize(&tree_a, &data_a, &len_a);
    tree_serialize(&tree_b, &data_b, &len_b);

    assert(len_a == len_b);
    assert(memcmp(data_a, data_b, len_a) == 0);

    free(data_a);
    free(data_b);

    printf("PASS: tree deterministic serialization\n");
}

void test_tree_object_store(void) {
    Tree tree;
    tree.count = 2;
    tree.entries[0].mode = 0100644;
    memset(tree.entries[0].hash.hash, 0xDD, HASH_SIZE);
    strcpy(tree.entries[0].name, "a.txt");
    tree.entries[1].mode = 0100644;
    memset(tree.entries[1].hash.hash, 0xEE, HASH_SIZE);
    strcpy(tree.entries[1].name, "b.txt");

    void *raw;
    size_t raw_len;
    assert(tree_serialize(&tree, &raw, &raw_len) == 0);

    ObjectID id;
    assert(object_write(OBJ_TREE, raw, raw_len, &id) == 0);
    free(raw);

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);
    printf("Stored tree object hash: %s\n", hex);

    char path[512];
    object_path(&id, path, sizeof(path));
    printf("Object stored at: %s\n", path);

    ObjectType type;
    void *read_back;
    size_t read_len;
    assert(object_read(&id, &type, &read_back, &read_len) == 0);
    assert(type == OBJ_TREE);

    Tree parsed;
    assert(tree_parse(read_back, read_len, &parsed) == 0);
    assert(parsed.count == 2);
    assert(strcmp(parsed.entries[0].name, "a.txt") == 0);
    assert(strcmp(parsed.entries[1].name, "b.txt") == 0);
    free(read_back);

    printf("PASS: tree object store roundtrip\n");
}

int main(void) {
    int rc __attribute__((unused));
    rc = system("rm -rf .pes");
    rc = system("mkdir -p .pes/objects .pes/refs/heads");

    test_tree_roundtrip();
    test_tree_determinism();
    test_tree_object_store();

    printf("\nAll Phase 2 tests passed.\n");
    return 0;
}

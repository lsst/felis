Modify Tap Example
==================

With felis installed, you can try out the modify-tap command in this directory.

### Option `--start-schema-at=<index>`

This command will modify `tap:schema_index` when encountered as 0, missing, or greater
than index, and assign it a new index, starting at `<index>` and increasing in the order it is
encountered.

```
felis modify-tap --start-schema-at=10 point.yml locations.yml data_release.yml
```

The command can be pipelines with a merge command:

```
felis modify-tap --start-schema-at=10 point.yml locations.yml data_release.yml |\
     felis merge - ../merge/point-extra.yml
```

Which can also be pipelined into the `load-tap` command:

```
felis modify-tap --start-schema-at=10 point.yml locations.yml data_release.yml |\
     felis merge - ../merge/point-extra.yml |\
     felis load-tap --dry-run --engine-url=sqlite:// -
```

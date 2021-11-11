Merge Example
==================

With felis installed, you can try out the `merge` command in this directory.

You can merge a felis schema file with extra annotations in a graph format:

```
felis merge point.yml point-extra.yml
```

The graph annotations will overwrite properties for a given `@id` only.
They appear like this (see `point-extra.yml`):
```

---
"@graph":
  - "@id": "#Geometry_Point.ra"
    ivoa:ucd: pos.eq.ra;meta.main
    fits:tunit: deg
  - "@id": "#Geometry_Point.dec"
    ivoa:ucd: pos.eq.dec;meta.main
    fits:tunit: deg
```

The output of merge can be pipelined into `load-tap`

```
felis merge point.yml point-extra.yml |\
     felis load-tap --dry-run --engine-url=sqlite:// -
```

It is also possible to merge multiple schemas as well, in addition to pipelining them
into `load-tap`:

```
felis merge point.yml point-extra.yml locations.yml |\
     felis load-tap --dry-run --engine-url=sqlite:// -
```
# Generates a report on selected attributes in a Rubin sdm_schemas Felis file
# Assumes that the @id attribute is always first
# Assumes that the attribute: and value are always separated by a blank
#
# FIXME - must be replaced by the use of real YAML-syntax-aware tooling
#
# Input variables:
#    target: regular expression for values of @id
#    attrs:  blank-delimited list of regular expressions for attributes of interest
#            (unexpected behavior will result if a regex matches multiple attributes)
#    OFS:    set to "," for CSV output; otherwise human-readable
#    DEBUG:  set to 1 for debug printout
#
# Example:
#    awk -v 'target=DiaObj.*lux' -v 'attrs=tunit ucd' -f report.awk
#
# Copyright 2023 California Institute of Technology

# Produce a line of output for column 'c'
function outrec( c ) {
        # Only produce output for the selected records
        if ( match( c, target ) ) {
          outstr = c;
          # loop over the requested attributes in the order provided
          # NB: the order of "for (ia in attarr)" is not guaranteed
          for ( ia=1; ia<=length(attarr); ia++ ) {
            val = attval[ia];
            if ( val == "" ) { val = "NULL"; }
            outstr = outstr OFS val;
          }
          print outstr;
        }
        return match( c, target );
      }

BEGIN {
        na = split(attrs, attarr, " ");
        col = "";

        if ( DEBUG ) {
          for ( ia in attarr ) {
            print ia, attarr[ia];
          }
        }
      }

# The "name" record is the start of a new "paragraph" in the YAML
/^ *- name:/ {
        # If this is not the first one, generate the output for the
        # previous "paragraph"
        if ( col != "" ) {
          outrec( col );
        }

        # Reset variables describing the current "paragraph"
        nam = $3;
        col = "";
        delete attval;
      }

# Save the ID as a label for the "paragraph"
/['"]@id['"]:/ {
        col = $2;
      }

# Process all other records
      {
        if ( DEBUG ) { print "SCAN:", $0; }
        for ( ia in attarr ) {
          if ( DEBUG ) { print "TEST:", attarr[ia]; }
          # If it's one of the selected attributes, save its value
          if ( match( $1, attarr[ia] ) ) {
            attrpos = match( $0, attarr[ia] "[^:]*: " );
            val = substr( $0, RSTART + RLENGTH );
            if ( attval[ia] == "" ) { attval[ia] = val; }
            # If there are multiple matches, concatenate them together with "/"
            # (This is probably rarely useful.)
            else                    { attval[ia] = attval[ia] "/" val; }
            if ( DEBUG ) { print "matched on", $1, attval[ia] }
          }
        }
      }

# If we reach the end, close out the last record
END   {
        if ( col != "" ) {
          outrec( col );
        }
}

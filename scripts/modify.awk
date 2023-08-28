# Modifies selected attributes in a Rubin sdm_schemas Felis file
# Assumes that the @id attribute is always first
# Assumes that the attribute: and value are always separated by a blank
# No understanding of the nested structure of YAML or any other YAML syntax
#
# WARNING - can produce totally nonsensical output if the input doesn't
# look exactly like the current "house style" for sdm_schemas Felis.
#
# FIXME - must be replaced by the use of real YAML-syntax-aware tooling
#
# Input variables:
#    target: regular expression for values of @id
#    attr:   regular expression for attribute of interest
#    newval: new value for the selected attribute
#    addnew: if 1, add a new attribute named exactly with the 
#            value of "attr" if no match is found; if 0, leave
#            records alone that do not already have that attribute.
#    DEBUG:  set to 1 for debug printout
#
# Example:
#    awk -v 'target=DiaObj.*lux' -v 'attr=ucd' -v 'newval=phot.flux' -f report.awk
#
# Copyright 2023 California Institute of Technology

# Wrap-up processing after the last attribute for a record
function lastattr( c ) {
        # Only produce output if "addnew" has been selected, and only
        # for the selected records, and only if the desired attribute
        # was not found.
        if ( addnew && match( c, target ) && !found ) {
          print substr( spaces, 1, indent ) attr ": " newval;
        }
        return match( c, target );
      }

BEGIN {
        nam = "";
        col = "";
        indent = 0;
        found = 0;
        spaces = "                                     ";
      }

# The "name" record is the start of a new "paragraph" in the YAML
/^ *- name:/ {
        # If this is not the first one, generate any remaining output for the
        # previous "paragraph"
        if ( col != "" ) {
          lastattr( col );
        }

        # Reset variables describing the current "paragraph"
        nam = $3;
        col = "";
        indent = 0;
        found = 0;
      }

# Save the ID as a label for the "paragraph"
/"@id":/ {
        col = $2;
        # infer the appropriate indentation for this record from this line
        indent = match( $0, "[^ ]" )-1;
      }

# Process all other records
      {
        changed = 0;
        if ( match( col, target ) ) {
          if ( DEBUG ) { print "SCAN:", $0; }
          if ( match( $1, attr ) ) {
            valpos = match( $0, attr ".*:" );
            print substr( $0, 1, RSTART+RLENGTH-1 ) " " newval;
            found = 1;
            changed = 1;
          }
        }
        if ( !changed ) { print; } # pass through unchanged
      }

# If we reach the end, close out the last record
END   {
        if ( col != "" ) {
          lastattr( col );
        }
}

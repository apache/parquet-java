fail_the_build=
reduced_pom="$(tempfile)"
shopt -s globstar # Enables ** to match files in subdirectories recursively
for pom in **/pom.xml
do
  # Removes the project/version and project/parent/version elements, because
  # those are allowed to have SNAPSHOT in them. Also removes comments.
  xmlstarlet ed -N pom='http://maven.apache.org/POM/4.0.0' \
             -d '/pom:project/pom:version|/pom:project/pom:parent/pom:version|//comment()' "$pom" > "$reduced_pom"
  if grep -q SNAPSHOT "$reduced_pom"
  then
    if [[ ! "$fail_the_build" ]]
    then
      printf "Error: POM files in the master branch can not refer to SNAPSHOT versions.\n"
      fail_the_build=YES
    fi
    printf "\nOffending POM file: %s\nOffending content:\n" "$pom"
    # Removes every element that does not have SNAPSHOT in it or its
    # descendants. As a result, we get a skeleton of the POM file with only the
    # offending parts.
    xmlstarlet ed -d "//*[count((.|.//*)[contains(text(), 'SNAPSHOT')]) = 0]" "$reduced_pom"
  fi
done
rm "$reduced_pom"
if [[ "$fail_the_build" ]]
then
   exit 1
fi

shopt -s globstar
fail_the_build=
reduced_pom="$(tempfile)"
for pom in **/pom.xml
do
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
    xmlstarlet ed -d "//*[count((.|.//*)[contains(text(), 'SNAPSHOT')]) = 0]" "$reduced_pom"
  fi
done
rm "$reduced_pom"
if [[ "$fail_the_build" ]]
then
   exit 1
fi

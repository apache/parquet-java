# Committers (in aplhabetical order):

| Name               | Apache Id  | github id   | Area of expertise                |
|--------------------|------------|-------------|----------------------------------|
| Aniket Mokashi     | aniket486  |             | Pig, Thrift, Encodings           |
| Brock Noland       | brock      |             | Hive                             |
| Chris Aniszczyk    | caniszczyk |             |                                  |
| Dmitriy Ryaboy     | dvryaboy   |             |                                  |
| Jake Farrell       | jfarrell   |             |                                  |
| Jonathan Coveney   | jcoveney   |             |                                  |
| Julien Le Dem      | julien     | julienledem | encodings, assembly, Thrift, Pig |
| Lukas Nalezenec    | lukas      |             | ProtoBuf                         |
| Marcel Kornacker   | marcel     |             | format                           |
| Mickael Lacour     | mlacour    |             | Hive                             |
| Nong Li            | nong       |             | format, Snappy, cpp              |
| Remy Pecqueur      | rpecqueur  |             | Hive                             |
| Ryan Blue          | blue       |             | Avro                             |
| Tianshuo Deng      | tianshuo   |             | Thrift, Scrooge, Projection      |
| Tom White          | tomwhite   |             | Avro                             |
| Wesley Graham Peck | wesleypeck |             | tools                            |

Reviewing guidelines:
Committers have the responsibility to give constructive and timely feedback on the pull requests.
Anybody can give feedback on a pull request but only committers can merge it.

First things to look at in a Pull Request:
 - Is there a corresponding JIRA, and is it mentioned in the description? If not ask the contributor to make one.
 - If a JIRA is open, make sure it is assigned to the contributor. (they need to have the contributor role here: https://issues.apache.org/jira/plugins/servlet/project-config/PARQUET/roles)
 - Is it an uncontroversial change that looks good? => merge it
 - Is it something that requires the attention of committers with a specific expertise? => mention those committers by their github id in the pull request.

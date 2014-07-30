# Committers (in aplhabetical order):

| Name               | Apache Id  | github id   | Area of expertise                |
|--------------------|------------|-------------|----------------------------------|
| Aniket Mokashi     | aniket486  | aniket486   | Encodings           |
| Brock Noland       | brock      | brockn      |
| Chris Aniszczyk    | caniszczyk |             | build, maven                     |
| Dmitriy Ryaboy     | dvryaboy   | dvryaboy    | 
| Jake Farrell       | jfarrell   |             |                                  |
| Jonathan Coveney   | jcoveney   | jcoveney    |                                  |
| Julien Le Dem      | julien     | julienledem | encodings, assembly|
| Lukas Nalezenec    | lukas      | lukasnalezenec | ProtoBuf                         |
| Marcel Kornacker   | marcel     |             | format                           |
| Mickael Lacour     | mlacour    | mickaellcr  |
| Nong Li            | nong       |             | format, Snappy, cpp              |
| Remy Pecqueur      | rpecqueur  |             |
| Ryan Blue          | blue       | rdblue      |
| Tianshuo Deng      | tianshuo   | tsdeng      | Scrooge, Projection      |
| Tom White          | tomwhite   | tomwhite    |
| Wesley Graham Peck | wesleypeck | wesleypeck  |

Reviewing guidelines:
Committers have the responsibility to give constructive and timely feedback on the pull requests.
Anybody can give feedback on a pull request but only committers can merge it.

First things to look at in a Pull Request:
 - Is there a corresponding JIRA, and is it mentioned in the description? If not ask the contributor to make one.
 - If a JIRA is open, make sure it is assigned to the contributor. (they need to have the contributor role here: https://issues.apache.org/jira/plugins/servlet/project-config/PARQUET/roles)
 - Is it an uncontroversial change that looks good (has apropriate tests and the build is succesful)? => merge it
 - Is it something that requires the attention of committers with a specific expertise? => mention those committers by their github id in the pull request.

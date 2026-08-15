UPDSVC.R4X
===========

UPDSVC ist der einzige Hintergrunddienst fuer die R4OS-Clientupdates.
Der versionierte Endpoint `UPDSVC` besitzt Status, Suche, Download,
Installation, Update All, Abbruch und eine paginierte Ergebnissicht.

Der Endpoint fuehrt keine langen Arbeiten aus. Er nimmt genau einen
durablen Auftrag an; ein einzelner ProgramThread verarbeitet diesen Auftrag.
Weitere Clients erhalten `busy` und koennen keinen zweiten Netzwerk- oder
Installationslauf erzeugen. Status und Job-ID werden in
`C:\R4OS\UPDATE\STAGED\UPDSVC.R4S` rekonstruktionsfaehig gesichert.

Installationen rufen die gemeinsame `system_update_engine` direkt auf.
UPDSVC startet weder SYSUPD noch wertet es Konsolenausgaben aus. Damit gilt
fuer Dienst und Terminal derselbe `SYSLOCK.LCK`-Vertrag. Ein Update von
UPDSVC selbst muss als Neustartpaket angeboten werden; der Dienst versucht
nie, sich selbst zu stoppen oder neu zu starten.

Seit 0.63.27 erbt UPDSVC von dieser gemeinsamen Engine begrenzte Retries fuer
transiente Paketinformationen, bekannte Dateilaengen und private
Stage-Streams. Seit 0.63.29 wird dabei auch ein scheinbarer Dateinichtfund erst
nach drei Metadatenversuchen als dauerhaft bewertet. Seit 0.63.30 wird ein
transient fehlgeschlagener, identitaetsgebundener Stage-Abort ebenfalls
begrenzt wiederholt und sein Ergebnis gegen die erwarteten Payloadbytes
geprueft. Ein dauerhafter I/O-Fehler bleibt ein fehlgeschlagener Auftrag;
ungueltige Pakete und fremde Konfliktdateien werden niemals blind wiederholt
oder geloescht.

Eine an einen aktuellen Suchsnapshot gebundene Einzelinstallation bestaetigt
den Zielrelease genau dann journalisiert, wenn danach das komplette lokale
Profil erreicht ist und kein Neustart aussteht. Eine spaetere leere Suche
holt diesen Abschluss nach, falls Dienst oder System zwischen dem letzten
Live-Artefakt und der Releasebestaetigung unterbrochen wurden. Administrative
Installationen ohne passenden Suchsnapshot duerfen die Releaseversion nicht
veraendern.

Seit 0.63.30 darf ein bereits heruntergeladenes Paket in einer spaeteren
Suche wiederverwendet werden, wenn ID, Paket- und Releaseversion, Dateiname,
Groesse, SHA-256 und Download-URL exakt demselben unveraenderlichen Angebot
entsprechen. Die Installation bindet es dann an die neue Such-ID und deren
aktuellen Ergebnisindex. Jobstatus persistiert beide Werte; ein neuer Job
setzt sie immer explizit, sodass eine Zeile nicht im alten `Installing`-
Zustand stehenbleibt, waehrend der Gesamtstatus bereits `staged` meldet.

Die nicht einkompilierte Verbindungskonfiguration liegt unter
`C:\R4OS\CONFIG\UPDATE.R4S`. UPDSVC gehoert zum Slim-Profil und wird ueber
`SERVICES.R4S` automatisch gestartet.

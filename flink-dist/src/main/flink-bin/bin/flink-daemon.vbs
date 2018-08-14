
Set Win32_Process = GetObject("winmgmts:Win32_Process")
Set fso = CreateObject("Scripting.FileSystemObject")
Set WMIService = GetObject("winmgmts:{impersonationLevel=impersonate}")
Set WScript_Shell = CreateObject( "WScript.Shell" )
Set Win32_ProcessStartup = GetObject("winmgmts:Win32_ProcessStartup")
strComputerName = WScript_Shell.ExpandEnvironmentStrings( "%COMPUTERNAME%" )
usage = "Usage: flink-daemon.bat (start|stop|stop-all) (jobmanager|taskmanager|historyserver|zookeeper) [args]"

If Wscript.Arguments.Count < 2 Then
    Wscript.Echo usage
    WScript.Quit 1
End If

startstop = WScript.Arguments.Item(0)
daemon = WScript.Arguments.Item(1)

Select case daemon
    case "jobmanager"
        CLASS_TO_RUN = "org.apache.flink.runtime.jobmanager.JobManager"
    case "taskmanager"
        CLASS_TO_RUN = "org.apache.flink.runtime.taskmanager.TaskManager"
    case "taskexecutor"
        CLASS_TO_RUN = "org.apache.flink.runtime.taskexecutor.TaskManagerRunner"
    case "historyserver"
        CLASS_TO_RUN = "org.apache.flink.runtime.webmonitor.history.HistoryServer"
    case "zookeeper"
        CLASS_TO_RUN = "org.apache.flink.runtime.zookeeper.FlinkZooKeeperQuorumPeer"
    case "standalonesession"
        CLASS_TO_RUN = "org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint"
    case else
        Wscript.Echo "Unknown daemon " & daemon & ". " & usage
        WScript.Quit 1
End select

binDir = fso.GetParentFolderName(WScript.ScriptFullName)
FLINK_ROOT_DIR = fso.GetParentFolderName(binDir)
FLINK_LIB_DIR = FLINK_ROOT_DIR & "\lib"
FLINK_CLASSPATH = FLINK_LIB_DIR & "\*"
FLINK_CONF_DIR = FLINK_ROOT_DIR & "\conf"
FLINK_LOG_DIR = FLINK_ROOT_DIR & "\log"
JVM_ARGS = "-Xms1024m -Xmx1024m"


Select case startstop
    case "start"
        'Print a warning if daemons are already running on host
        Set colProcess = WMIService.ExecQuery("Select * from Win32_Process")

        count = 0
        For Each objProcess in colProcess
            if objProcess.Caption = "java.exe" And InStr(objProcess.CommandLine, CLASS_TO_RUN) Then
                count = count +1
                'Wscript.Echo objProcess.Caption & " | " & objProcess.processId & " | " & objProcess.CommandLine
            End If
        Next

        If count > 0 Then
            Wscript.Echo "[INFO] " & count & " instance(s) of " & daemon & " are already running on " & strComputerName & "."
        End If

        strUserName = WScript_Shell.ExpandEnvironmentStrings( "%USERNAME%" )
        logname_tm = "flink-" & strUserName & "-taskmanager." & count & ".log"
        log_tm = FLINK_LOG_DIR & "\" & logname_tm
        outname_tm = "flink-" & strUserName & "-taskmanager." & count & ".out"
        out_tm = FLINK_LOG_DIR & "\" & outname_tm
        log_setting_tm = "-Dlog.file=""" & log_tm & """ -Dlogback.configurationFile=file:""" & FLINK_CONF_DIR & "\logback.xml"" -Dlog4j.configuration=file:""" & FLINK_CONF_DIR & "\log4j.properties"""

        'logrotate
        For i = 5 to 0 Step -1
            j = i + 1
            fn = FLINK_LOG_DIR & "\" & logname_tm & "." & i
            If fso.FileExists(fn) Then fso.MoveFile fn, FLINK_LOG_DIR & "\" & logname_tm & "." & j
            fn = FLINK_LOG_DIR & "\" & outname_tm & "." & i
            if fso.FileExists(fn) Then fso.MoveFile fn, FLINK_LOG_DIR & "\" & outname_tm & "." & j
        Next
        fn = FLINK_LOG_DIR & "\" & logname_tm
        if fso.FileExists(fn) Then fso.MoveFile fn, FLINK_LOG_DIR & "\" & logname_tm & ".0"
        fn = FLINK_LOG_DIR & "\" & outname_tm
        if fso.FileExists(fn) Then fso.MoveFile fn, FLINK_LOG_DIR & "\" & outname_tm & ".0"
        fn = FLINK_LOG_DIR & "\" & logname_tm & ".6"
        if fso.FileExists(fn) Then fso.DeleteFile fn
        fn = FLINK_LOG_DIR & "\" & outname_tm & ".6"
        if fso.FileExists(fn) Then fso.DeleteFile fn

        Wscript.Echo "Starting " & daemon & " daemon on host " & strComputerName & "."
        cmd = "cmd /c java " & JVM_ARGS & " " & log_setting_tm & " -cp """ & FLINK_CLASSPATH & """; " & CLASS_TO_RUN & " --configDir """ & FLINK_CONF_DIR & """ "_
            & ARGS & " 1> """ & out_tm & """ 2>&1"
        'Wscript.Echo cmd
        Set objConfig = Win32_ProcessStartup.SpawnInstance_
        objConfig.ShowWindow = 0
        If Win32_Process.Create(cmd,null,objConfig,processid) <> 0 Then Wscript.Echo "Error starting " & daemon & " daemon."

    case "stop"
        Set colProcess = WMIService.ExecQuery("Select * from Win32_Process")

        process = 0
        For Each objProcess in colProcess
            if objProcess.Caption = "java.exe" And InStr(objProcess.CommandLine, CLASS_TO_RUN) Then
                'Wscript.Echo objProcess.Caption & " | " & objProcess.processId & " | " & objProcess.CommandLine
                Set process = objProcess
                Exit For
            End If
        Next

        If Not IsObject(process) Then
            Wscript.Echo "No " & daemon & " daemon to stop on host " & strComputerName & "."
        Else
            Wscript.Echo "Stopping " & daemon & " daemon (pid: " & process.processId & ") on host " & strComputerName & "."
            process.Terminate
        End If

    case "stop-all"
        Set colProcess = WMIService.ExecQuery("Select * from Win32_Process")

        For Each objProcess in colProcess
            if objProcess.Caption = "java.exe" And InStr(objProcess.CommandLine, CLASS_TO_RUN) Then
                Wscript.Echo "Stopping " & daemon & " daemon (pid: " & objProcess.processId & ") on host " & strComputerName & "."
                'Wscript.Echo objProcess.Caption & " | " & objProcess.processId & " | " & objProcess.CommandLine
                objProcess.Terminate
            End If
        Next

    case else
        Wscript.Echo "Unexpected argument '" & startstop & "'. " & usage
        WScript.Quit 1

End select

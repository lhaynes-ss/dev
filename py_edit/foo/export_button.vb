Private Sub CommandButton1_Click()

    
    Dim currentWorkbook As Workbook
    Dim worker As String
    Dim fname As String
    Dim path As String
    Dim wasUpdated As Boolean

    Dim originalFilePath As String
    Dim originalFileName As String

    originalFilePath = ThisWorkbook.FullName
    originalFileName = ThisWorkbook.Name
    wasUpdated = FALSE


    path = Left(originalFilePath, Len(originalFilePath) - Len(originalFileName))

    Set currentWorkbook = ThisWorkbook

    Application.DisplayAlerts = False

    With currentWorkbook.Worksheets("Settings")
    
        For Each s In Range("staff[Name]")

            worker = s.Value
            ' Debug.Print worker

            IF worker <> "" THEN 
                Range("B5").Value = worker

                fname = path & TRIM(worker) & ".xlsx"
                fname = Replace(fname, " ", "_")
                ' Debug.Print fname

                currentWorkbook.Worksheets("Admin").Visible = False
                currentWorkbook.Worksheets("Settings").Visible = False

                currentWorkbook.SaveAs Filename:= fname, FileFormat := 51

                currentWorkbook.Worksheets("Settings").Visible = True

                wasUpdated = TRUE
            END IF

        Next s

    End With

    IF wasUpdated = TRUE THEN 
        currentWorkbook.SaveAs Filename:= originalFilePath, FileFormat := 52
    ELSE
        MsgBox "No files were generated"
    END IF

    Application.DisplayAlerts = True


    MsgBox "The timesheets have been created"


End Sub
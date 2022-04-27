from reg_data import Complaints, Investigations, Recalls, CPSCReports, CPSCNov
import os
import shutil


def wrap_html(results):
    with open(fr'C:\Users\jhayes\Documents\nhtsa query data\update.html', 'w') as text_file:
        html_style = """\
        <html>
        <head><style>
            h2 {
                text-align: center;
                font-family: Helvetica, Arial, sans-serif;
            }
            body {
                margin:5%
                margin-right:5%
            }
            table { 
                margin-left: 5%;
                margin-right: 5%;
            }
            table, th, td {
                border: 1px solid black;
                border-collapse: collapse;
            }
            th, td {
                padding: 5px;
                text-align: center;
                font-family: Helvetica, Arial, sans-serif;
                font-size: 90%;
            }
            table tbody tr:hover {
                background-color: #dddddd;
            }
            .wide {
                width: 90%; 
            }
        </style></head>
        <body>
        """
        text_file.write(html_style)

        for result_key in results.keys():
            text_file.write(f'''<html><H1>{result_key}</H1></html>''')
            text_file.write(results[result_key])

        text_file.write(f'''</body> </html>''')

    return


if __name__ == '__main__':
    try:
        shutil.copy(fr'C:\Users\{os.getlogin()}\Documents\nhtsa query data\update.html',fr'C:\Users\{os.getlogin()}\Documents\nhtsa query data\updateLast.html')
    except:
        pass
    results_html = {}
    development_mode = False

    complaints = Complaints(update_window=60, development_mode=development_mode)
    complaints_df = complaints.run_update(mode='recent')
    complaints_html = complaints.write_html(complaints_df)
    results_html.update({'Complaints': complaints_html})

    investigations = Investigations(update_window=60, development_mode=development_mode)
    investigations_df = investigations.run_update(mode='recent')
    investigations_html = investigations.write_html(investigations_df)
    results_html.update({'Investigations': investigations_html})

    recalls = Recalls(update_window=720, development_mode=development_mode)
    recall_df = recalls.run_update(mode='recent')
    recall_html = recalls.write_html(recall_df)
    results_html.update({'Recalls': recall_html})

    cpsc_reports = CPSCReports(update_window=60, development_mode=development_mode)
    cpsc_reports_df = cpsc_reports.run_update(mode='recent')
    cpsc_reports_html = cpsc_reports.write_html(cpsc_reports_df)
    results_html.update({'CPSC Reports': cpsc_reports_html})

    cpsc_nov = CPSCNov(update_window=60, development_mode=development_mode)
    cpsc_nov_df = cpsc_nov.run_update()
    cpsc_nov_html = cpsc_nov.write_html(cpsc_nov_df)
    results_html.update({'CPSC Notice of Violation': cpsc_nov_html})

    wrap_html(results_html)

import { DownloadIcon, FileTextIcon } from "lucide-react";
import { Button } from "#/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "#/components/ui/card";

type WidgetReportDownloadProps = {
  title: string;
  download_url?: string;
  message: string;
};

export function WidgetReportDownload({ title, download_url, message }: WidgetReportDownloadProps) {
  const handleDownload = () => {
    if (!download_url) {
      return;
    }

    const anchor = document.createElement("a");
    anchor.href = download_url;
    anchor.download = `${title.replace(/[^\w\-]/g, "_")}.docx`;
    anchor.click();
  };

  return (
    <Card>
      <CardHeader className={"flex-row items-center gap-3"}>
        <div className={"flex size-10 items-center justify-center rounded-md bg-primary/10"}>
          <FileTextIcon className={"size-5 text-primary"} />
        </div>
        <div className={"flex flex-col"}>
          <CardTitle className={"text-sm"}>{title}</CardTitle>
          <p className={"text-muted-foreground text-xs"}>{message}</p>
        </div>
      </CardHeader>
      {download_url && (
        <CardContent className={"pt-0"}>
          <Button onClick={handleDownload} size={"sm"} variant={"outline"}>
            <DownloadIcon />
            Download DOCX
          </Button>
        </CardContent>
      )}
    </Card>
  );
}

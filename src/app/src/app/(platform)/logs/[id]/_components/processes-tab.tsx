"use client";

import { format } from "date-fns";
import { AlertCircleIcon, CheckCircle2Icon, ClockIcon, InfoIcon } from "lucide-react";
import { useState } from "react";

import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Progress } from "@/components/ui/progress";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Spinner } from "@/components/ui/spinner";
import type { LogProcess, ProcessResultDetails } from "@/lib/api/types";

interface ProcessesTabProps {
	processes: LogProcess[];
	isLoading: boolean;
	error: string | null;
}

export function ProcessesTab({ processes, isLoading, error }: ProcessesTabProps) {
	const [selectedProcess, setSelectedProcess] = useState<LogProcess | null>(null);

	if (isLoading) {
		return (
			<div className={"flex items-center justify-center py-12"}>
				<Spinner />
			</div>
		);
	}

	if (error !== null) {
		return (
			<Alert variant={"destructive"}>
				<AlertCircleIcon className={"size-4"} />
				<AlertTitle>Failed to load processes</AlertTitle>
				<AlertDescription>{error}</AlertDescription>
			</Alert>
		);
	}

	if (processes.length === 0) {
		return (
			<Empty className={"border"}>
				<EmptyHeader>
					<EmptyMedia variant={"icon"}>
						<ClockIcon />
					</EmptyMedia>
					<EmptyTitle>No processes yet</EmptyTitle>
					<EmptyDescription>Upload log files to start a preprocessing run.</EmptyDescription>
				</EmptyHeader>
			</Empty>
		);
	}

	return (
		<>
			<div className={"flex flex-col gap-2"}>
				{processes.map((process) => (
					<ProcessRow
						key={process.id}
						onViewDetails={process.result !== null ? () => setSelectedProcess(process) : undefined}
						process={process}
					/>
				))}
			</div>

			{selectedProcess !== null && selectedProcess.result !== null && (
				<ProcessDetailsDialog onClose={() => setSelectedProcess(null)} process={selectedProcess} />
			)}
		</>
	);
}

interface ProcessRowProps {
	process: LogProcess;
	onViewDetails?: () => void;
}

function ProcessRow({ process, onViewDetails }: ProcessRowProps) {
	const formattedDate = format(new Date(process.created_at), "MMM d, yyyy 'at' h:mm a");
	const generatedTableCount = process.result?.generated_tables?.length ?? 0;
	const isInProgress =
		process.status === "queued" || process.status === "classified" || process.status === "processing";

	function getStatusLabel(): string {
		if (process.status === "completed") {
			return generatedTableCount > 0
				? `Created ${generatedTableCount} ${generatedTableCount === 1 ? "table" : "tables"}`
				: "Ingestion complete";
		}
		if (process.status === "failed") return "Ingestion failed";
		if (process.status === "classified") return "Classified — ingestion starting…";
		if (process.status === "processing") return "Parsing and loading…";
		return "Queued for processing…";
	}

	return (
		<div className={"flex flex-col gap-1.5 rounded-md border p-4"}>
			<div className={"flex items-center gap-3"}>
				<ProcessStatusIcon status={process.status} />
				<div className={"flex flex-1 flex-col gap-0.5"}>
					<span className={"font-medium text-sm"}>{getStatusLabel()}</span>
					<span className={"text-muted-foreground text-xs"}>{formattedDate}</span>
				</div>
				<ProcessStatusBadge status={process.status} />
				{!isInProgress && onViewDetails !== undefined && (
					<Button className={"shrink-0"} onClick={onViewDetails} size={"sm"} variant={"ghost"}>
						<InfoIcon />
						Details
					</Button>
				)}
			</div>

			{process.status === "completed" && process.result !== null && process.result.generated_tables !== undefined && (
				<div className={"flex items-center gap-2 text-muted-foreground text-xs"}>
					<span>{process.result.generated_tables.length} table(s) generated</span>
					<span>&middot;</span>
					<span>{Math.round(process.result.confidence * 100)}% confidence</span>
					<span>&middot;</span>
					<span>{process.result.file_observations.length} file(s) analyzed</span>
				</div>
			)}

			{process.status === "failed" && process.error !== null && (
				<p className={"rounded bg-destructive/10 px-2 py-1 font-mono text-destructive text-xs"}>{process.error}</p>
			)}

			{process.result !== null && process.result.file_observations.length > 0 && (
				<ProcessFileList
					fileObservations={process.result.file_observations}
					generatedTables={process.result.generated_tables}
					processStatus={process.status}
				/>
			)}
		</div>
	);
}

interface ProcessStatusIconProps {
	status: string;
}

function ProcessStatusIcon({ status }: ProcessStatusIconProps) {
	if (status === "completed") {
		return <CheckCircle2Icon className={"size-4 shrink-0 text-green-500"} />;
	}
	if (status === "failed") {
		return <AlertCircleIcon className={"size-4 shrink-0 text-destructive"} />;
	}
	return <Spinner className={"size-4 shrink-0"} />;
}

interface ProcessStatusBadgeProps {
	status: string;
}

function ProcessStatusBadge({ status }: ProcessStatusBadgeProps) {
	if (status === "completed") {
		return <Badge variant={"secondary"}>Completed</Badge>;
	}
	if (status === "failed") {
		return <Badge variant={"destructive"}>Failed</Badge>;
	}
	if (status === "classified") {
		return <Badge variant={"outline"}>Classified</Badge>;
	}
	if (status === "processing") {
		return <Badge variant={"outline"}>Processing</Badge>;
	}
	return <Badge variant={"outline"}>Queued</Badge>;
}

interface ProcessDetailsDialogProps {
	process: LogProcess;
	onClose: () => void;
}

function ProcessDetailsDialog({ process, onClose }: ProcessDetailsDialogProps) {
	// result is guaranteed non-null by the caller
	const details = process.result as ProcessResultDetails;
	const confidencePercent = Math.round(details.confidence * 100);
	const formattedDate = format(new Date(process.created_at), "MMM d, yyyy 'at' h:mm a");

	return (
		<Dialog
			onOpenChange={(open) => {
				if (!open) onClose();
			}}
			open={true}
		>
			<DialogContent className={"flex max-h-[85vh] flex-col overflow-hidden sm:max-w-2xl"}>
				<DialogHeader>
					<DialogTitle>Process Details</DialogTitle>
					<DialogDescription>{formattedDate}</DialogDescription>
				</DialogHeader>
				<ScrollArea className={"flex-1"}>
					<div className={"flex flex-col gap-4 py-1 pr-4"}>
						<p className={"text-muted-foreground text-sm"}>{details.schema_summary}</p>

						<div className={"flex flex-col gap-1.5"}>
							<div className={"flex items-center justify-between"}>
								<span className={"font-medium text-muted-foreground text-xs"}>Confidence</span>
								<span className={"font-medium text-xs"}>{confidencePercent}%</span>
							</div>
							<Progress className={"h-1.5"} value={confidencePercent} />
						</div>

						<div className={"flex flex-col gap-1"}>
							<span className={"font-medium text-muted-foreground text-xs"}>Segmentation</span>
							<div className={"flex flex-wrap items-center gap-2"}>
								<Badge variant={"outline"}>{details.segmentation.strategy}</Badge>
								<span className={"text-muted-foreground text-xs"}>{details.segmentation.rationale}</span>
							</div>
						</div>

						{details.file_observations.length > 0 && (
							<div className={"flex flex-col gap-1.5"}>
								<span className={"font-medium text-muted-foreground text-xs"}>
									Files ({details.file_observations.length})
								</span>
								<div className={"flex flex-col divide-y rounded-md border"}>
									{details.file_observations.map((observation) => (
										<div className={"flex items-center gap-3 px-3 py-2"} key={observation.filename}>
											<span className={"flex-1 truncate font-mono text-xs"}>{observation.filename}</span>
											<Badge className={"shrink-0 text-xs"} variant={"secondary"}>
												{observation.detected_format}
											</Badge>
											<span className={"shrink-0 text-muted-foreground text-xs"}>{observation.line_count} lines</span>
										</div>
									))}
								</div>
							</div>
						)}

						<div className={"flex flex-col gap-1.5"}>
							<span className={"font-medium text-muted-foreground text-xs"}>
								Generated Tables ({details.generated_tables.length})
							</span>
							<div className={"flex flex-col divide-y rounded-md border"}>
								{details.generated_tables.map((table) => (
									<div className={"flex items-center gap-3 px-3 py-2"} key={table.table_name}>
										<span className={"flex-1 font-medium text-xs"}>{getGeneratedTableLabel(table)}</span>
										{table.is_normalized ? (
											<Badge className={"shrink-0 text-xs"} variant={"outline"}>
												Normalized
											</Badge>
										) : (
											table.file_name !== null && (
												<Badge className={"max-w-40 shrink-0 truncate text-xs"} variant={"outline"}>
													{table.file_name}
												</Badge>
											)
										)}
									</div>
								))}
							</div>
						</div>

						<div className={"flex flex-col gap-1.5"}>
							<span className={"font-medium text-muted-foreground text-xs"}>Columns ({details.columns.length})</span>
							<Accordion collapsible={true} type={"single"}>
								<AccordionItem className={"rounded-md border"} value={"columns"}>
									<AccordionTrigger className={"px-3 py-2 text-xs hover:no-underline"}>
										View {details.columns.length} inferred columns
									</AccordionTrigger>
									<AccordionContent>
										<div className={"flex flex-col divide-y"}>
											{details.columns.map((column) => (
												<div className={"flex items-start gap-3 px-3 py-2"} key={column.name}>
													<div className={"flex flex-1 flex-col gap-0.5"}>
														<span className={"font-medium font-mono text-xs"}>{column.name}</span>
														{column.description !== "" && (
															<p className={"text-muted-foreground text-xs"}>{column.description}</p>
														)}
													</div>
													<div className={"flex shrink-0 items-center gap-1"}>
														<Badge className={"font-mono text-xs"} variant={"outline"}>
															{column.sql_type}
														</Badge>
														<Badge className={"text-xs"} variant={"secondary"}>
															{column.kind}
														</Badge>
													</div>
												</div>
											))}
										</div>
									</AccordionContent>
								</AccordionItem>
							</Accordion>
						</div>

						{details.warnings.length > 0 && (
							<Alert variant={"destructive"}>
								<AlertCircleIcon className={"size-4"} />
								<AlertTitle>Warnings</AlertTitle>
								<AlertDescription className={"flex flex-col gap-1"}>
									{details.warnings.map((warning, index) => (
										<span key={index}>{warning}</span>
									))}
								</AlertDescription>
							</Alert>
						)}

						{details.assumptions.length > 0 && (
							<Alert>
								<InfoIcon className={"size-4"} />
								<AlertTitle>Assumptions</AlertTitle>
								<AlertDescription className={"flex flex-col gap-1"}>
									{details.assumptions.map((assumption, index) => (
										<span key={index}>{assumption}</span>
									))}
								</AlertDescription>
							</Alert>
						)}
					</div>
				</ScrollArea>
			</DialogContent>
		</Dialog>
	);
}

function getGeneratedTableLabel(table: ProcessResultDetails["generated_tables"][number]): string {
	if (table.is_normalized) {
		return "Normalized Logs";
	}

	if (table.file_name !== null) {
		return `Logs for ${table.file_name}`;
	}

	return table.table_name;
}

interface ProcessFileListProps {
	fileObservations: ProcessResultDetails["file_observations"];
	generatedTables: ProcessResultDetails["generated_tables"];
	processStatus: string;
}

function ProcessFileList({ fileObservations, generatedTables, processStatus }: ProcessFileListProps) {
	function getFileStatus(): "completed" | "failed" | "processing" | "classified" | "queued" {
		if (processStatus === "completed") return "completed";
		if (processStatus === "failed") return "failed";
		if (processStatus === "processing") return "processing";
		if (processStatus === "classified") return "classified";
		return "queued";
	}

	function findTableForFile(filename: string): ProcessResultDetails["generated_tables"][number] | null {
		return generatedTables.find((table) => table.file_name === filename) ?? null;
	}

	function getStatusBadgeVariant(
		status: "completed" | "failed" | "processing" | "classified" | "queued",
	): "secondary" | "destructive" | "outline" {
		if (status === "completed") return "secondary";
		if (status === "failed") return "destructive";
		return "outline";
	}

	const fileStatus = getFileStatus();

	return (
		<Accordion className={"w-full"} collapsible={true} type={"single"}>
			<AccordionItem className={"border-none"} value={"files"}>
				<AccordionTrigger className={"h-auto rounded-md border px-3 py-2 text-xs hover:bg-muted/50 hover:no-underline"}>
					<div className={"flex items-center gap-2"}>
						<span className={"font-medium"}>Files ({fileObservations.length})</span>
						<span className={"text-muted-foreground"}>View breakdown</span>
					</div>
				</AccordionTrigger>
				<AccordionContent>
					<div className={"flex flex-col divide-y rounded-md border"}>
						{fileObservations.map((observation) => {
							const matchedTable = findTableForFile(observation.filename);
							return (
								<div className={"flex items-center gap-3 px-3 py-2"} key={observation.filename}>
									<span className={"flex-1 truncate font-mono text-xs"}>{observation.filename}</span>
									<Badge className={"shrink-0 text-xs"} variant={getStatusBadgeVariant(fileStatus)}>
										{fileStatus.charAt(0).toUpperCase() + fileStatus.slice(1)}
									</Badge>
									{matchedTable !== null ? (
										<Badge className={"max-w-40 shrink-0 truncate text-xs"} variant={"outline"}>
											{matchedTable.table_name}
										</Badge>
									) : (
										<span className={"shrink-0 text-muted-foreground text-xs"}>No table generated</span>
									)}
								</div>
							);
						})}
					</div>
				</AccordionContent>
			</AccordionItem>
		</Accordion>
	);
}

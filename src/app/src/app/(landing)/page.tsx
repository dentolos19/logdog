import {
	ArrowRight,
	BarChart3,
	Braces,
	CheckCircle2,
	ChevronRight,
	Database,
	FileCode,
	FileText,
	RefreshCw,
	Search,
	Shield,
	Terminal,
	Zap,
} from "lucide-react";
import Link from "next/link";

import { Button } from "@/components/ui/button";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { cn } from "@/lib/utils";

const FEATURES = [
	{
		Icon: Zap,
		title: "Instant Schema Detection",
		description:
			"AI automatically infers schema from any log format. No configuration files, no regex patterns to write.",
	},
	{
		Icon: Database,
		title: "Unified Data Store",
		description:
			"All parsed logs land in a queryable SQLite store. Use built-in SQL search or the visual explorer.",
	},
	{
		Icon: Search,
		title: "Full-Text & Structured Search",
		description:
			"Search across every field, across every log entry. Regex, exact match, or natural language.",
	},
	{
		Icon: BarChart3,
		title: "Auto-Generated Analytics",
		description:
			"Error rates, latency distributions, traffic heatmaps — dashboards appear automatically as your logs are ingested.",
	},
	{
		Icon: Shield,
		title: "Anomaly Detection",
		description:
			"Surface unusual patterns and outliers automatically. Know about the incident before users report it.",
	},
	{
		Icon: RefreshCw,
		title: "Live Streaming",
		description:
			"Tail logs in real time from any source. Watch your structured rows appear as data flows in.",
	},
];

const STATS = [
	{ value: "100+", label: "Log formats supported" },
	{ value: "<50ms", label: "Parse latency" },
	{ value: "∞", label: "Schema flexibility" },
];

const STEPS = [
	{
		number: "01",
		title: "Upload or stream your logs",
		description:
			"Drag and drop log files, point to a directory, or connect a live stream. Any format, any source.",
		detail: "Supports files, stdin, Docker, Kubernetes, S3 buckets, and more",
	},
	{
		number: "02",
		title: "AI detects the schema",
		description:
			"Our model reads your log structure and extracts fields, types, and relationships — automatically.",
		detail:
			"Handles JSON, Apache, syslog, regex patterns, and entirely custom formats",
	},
	{
		number: "03",
		title: "Query, visualize, and act",
		description:
			"Explore structured rows, write SQL, and set up alerts on the patterns that matter to your system.",
		detail:
			"Export to CSV, webhook, or push to your existing observability stack",
	},
];

const STRUCTURED_FORMATS = [
	"JSON / NDJSON",
	"CSV / TSV",
	"Logfmt (key=value)",
	"OpenTelemetry",
	"Custom structured",
];

const SEMI_FORMATS = [
	"Apache / Nginx access logs",
	"Syslog / journald",
	"W3C Extended Log Format",
	"AWS CloudFront / ALB",
	"Kubernetes pod logs",
];

const UNSTRUCTURED_FORMATS = [
	"Legacy application output",
	"Error message dumps",
	"Crash reports",
	"Debug println() output",
	"Any custom text format",
];

export default function LandingPage() {
	return (
		<div className={"min-h-screen bg-[#070b0f] text-white overflow-x-hidden"}>
			{/* CSS Animations */}
			<style>{`
        @keyframes scroll-logs {
          0% { transform: translateY(0); }
          100% { transform: translateY(-50%); }
        }
        @keyframes fade-up {
          from { opacity: 0; transform: translateY(20px); }
          to { opacity: 1; transform: translateY(0); }
        }
        @keyframes blink-cursor {
          0%, 100% { opacity: 1; }
          50% { opacity: 0; }
        }
        .animate-scroll-logs {
          animation: scroll-logs 16s linear infinite;
        }
        .animate-fade-up {
          animation: fade-up 0.65s cubic-bezier(0.22, 1, 0.36, 1) forwards;
        }
        .cursor-blink::after {
          content: '█';
          animation: blink-cursor 1.1s step-end infinite;
          opacity: 0.7;
          margin-left: 2px;
          font-size: 0.8em;
        }
        .stagger-1 { animation-delay: 0.05s; opacity: 0; }
        .stagger-2 { animation-delay: 0.15s; opacity: 0; }
        .stagger-3 { animation-delay: 0.25s; opacity: 0; }
        .stagger-4 { animation-delay: 0.38s; opacity: 0; }
        .stagger-5 { animation-delay: 0.52s; opacity: 0; }
        .stagger-6 { animation-delay: 0.66s; opacity: 0; }
      `}</style>

			{/* Grid background */}
			<div
				className={"fixed inset-0 pointer-events-none select-none"}
				style={{
					backgroundImage:
						"linear-gradient(to right, rgba(255,255,255,0.035) 1px, transparent 1px), linear-gradient(to bottom, rgba(255,255,255,0.035) 1px, transparent 1px)",
					backgroundSize: "72px 72px",
				}}
			/>

			{/* Top amber glow */}
			<div
				className={"fixed inset-0 pointer-events-none select-none"}
				style={{
					background:
						"radial-gradient(ellipse 90% 45% at 50% -5%, rgba(245, 158, 11, 0.07) 0%, transparent 65%)",
				}}
			/>

			{/* ─── Nav ──────────────────────────────────────────────────── */}
			<nav
				className={
					"relative z-10 flex items-center justify-between px-6 py-4 max-w-7xl mx-auto"
				}
			>
				<div className={"flex items-center gap-2.5"}>
					<div
						className={
							"size-8 rounded-lg bg-amber-400 flex items-center justify-center shrink-0"
						}
					>
						<Terminal className={"size-4 text-zinc-900"} />
					</div>
					<span className={"text-lg font-bold tracking-tight"}>logdog</span>
				</div>

				<div
					className={"hidden md:flex items-center gap-7 text-sm text-zinc-400"}
				>
					<Link
						href={"#formats"}
						className={"hover:text-white transition-colors duration-150"}
					>
						Formats
					</Link>
					<Link
						href={"#how-it-works"}
						className={"hover:text-white transition-colors duration-150"}
					>
						How it works
					</Link>
					<Link
						href={"#features"}
						className={"hover:text-white transition-colors duration-150"}
					>
						Features
					</Link>
				</div>

				<div className={"flex items-center gap-3"}>
					<Button
						variant={"ghost"}
						size={"sm"}
						className={"text-zinc-400 hover:text-white hover:bg-zinc-800/80"}
						asChild
					>
						<Link href={"/auth"}>Sign in</Link>
					</Button>
					<Button
						size={"sm"}
						className={
							"bg-amber-400 text-zinc-900 hover:bg-amber-300 font-semibold shadow-none"
						}
						asChild
					>
						<Link href={"/auth/new"}>Get started</Link>
					</Button>
				</div>
			</nav>

			{/* ─── Hero ──────────────────────────────────────────────────── */}
			<section
				className={
					"relative z-10 px-6 pt-20 pb-28 max-w-7xl mx-auto text-center"
				}
			>
				<h1
					className={
						"animate-fade-up stagger-2 text-5xl md:text-[4.5rem] lg:text-[5.5rem] font-extrabold tracking-tight leading-[1.04] mb-6"
					}
				>
					Parse any log.
					<br />
					<span className={"text-amber-400"}>Understand everything.</span>
				</h1>

				<p
					className={
						"animate-fade-up stagger-3 text-lg md:text-xl text-zinc-400 max-w-2xl mx-auto mb-10 leading-relaxed"
					}
				>
					Drop in your raw logs — structured, unstructured, or anything in
					between. Logdog detects the schema, extracts the fields, and surfaces
					insights you can act on. No config files. No regex wrangling.
				</p>

				<div
					className={
						"animate-fade-up stagger-4 flex flex-col sm:flex-row gap-3 justify-center mb-20"
					}
				>
					<Button
						size={"lg"}
						className={
							"bg-amber-400 text-zinc-900 hover:bg-amber-300 font-semibold gap-2 h-12 px-7 text-base shadow-none"
						}
						asChild
					>
						<Link href={"/auth/new"}>
							Start parsing free
							<ArrowRight className={"size-4"} />
						</Link>
					</Button>
					<Button
						size={"lg"}
						variant={"outline"}
						className={
							"border-zinc-700 bg-transparent text-zinc-300 hover:bg-zinc-800/80 hover:text-white h-12 px-7 text-base"
						}
						asChild
					>
						<Link href={"#how-it-works"}>See how it works</Link>
					</Button>
				</div>

				{/* Hero terminal */}
				<div className={"animate-fade-up stagger-5 relative max-w-5xl mx-auto"}>
					<div
						className={"absolute -inset-12 rounded-3xl pointer-events-none"}
						style={{
							background:
								"radial-gradient(ellipse at center, rgba(245,158,11,0.055) 0%, transparent 65%)",
						}}
					/>

					<div
						className={
							"relative grid md:grid-cols-2 gap-0 rounded-2xl overflow-hidden border border-zinc-800 bg-zinc-950/90 shadow-2xl"
						}
					>
						{/* Raw input pane */}
						<div className={"border-r border-zinc-800"}>
							<div
								className={
									"flex items-center gap-2 px-4 py-3 border-b border-zinc-800 bg-zinc-900/60"
								}
							>
								<div className={"flex gap-1.5"}>
									<div className={"size-3 rounded-full bg-zinc-700"} />
									<div className={"size-3 rounded-full bg-zinc-700"} />
									<div className={"size-3 rounded-full bg-zinc-700"} />
								</div>
								<span className={"text-xs text-zinc-500 ml-2 font-mono"}>
									raw.log
								</span>
								<span
									className={
										"ml-auto text-[10px] font-mono px-1.5 py-0.5 rounded bg-zinc-800 text-zinc-500"
									}
								>
									INPUT
								</span>
							</div>

							<div className={"h-64 overflow-hidden relative px-5 py-4"}>
								<div
									className={
										"animate-scroll-logs font-mono text-[11px] leading-[1.7] text-zinc-500 whitespace-nowrap"
									}
								>
									{[...Array(2)].map((_, index) => (
										<div key={index}>
											<p>
												<span className={"text-zinc-600"}>[14:22:31]</span>{" "}
												<span className={"text-red-400"}>CRIT</span> auth-worker
												OOM killed after 47 open connections
											</p>
											<p>
												<span className={"text-zinc-600"}>192.168.1.2</span> -
												alice [05/Mar/2026:14:22:31]{" "}
												<span className={"text-zinc-400"}>
													"POST /api/login HTTP/1.1"
												</span>{" "}
												<span className={"text-amber-400"}>401</span> 1247
											</p>
											<p>
												<span
													className={"text-zinc-500"}
												>{`{"ts":"2026-03-05","lvl":"WARN","svc":"payments","msg":"retry #3","dur":892}`}</span>
											</p>
											<p>
												<span className={"text-zinc-600"}>Mar 5 14:22:31</span>{" "}
												prod-node-4 kernel:{" "}
												<span className={"text-red-400/80"}>OOM</span>: kill
												event pid=4729 process=node
											</p>
											<p>
												<span className={"text-zinc-600"}>[14:22:32]</span>{" "}
												<span className={"text-green-400"}>INFO</span> db-worker
												query completed 23ms rows=1024
											</p>
											<p>
												<span className={"text-zinc-600"}>10.0.0.5</span> - -{" "}
												<span className={"text-zinc-400"}>
													"GET /health HTTP/1.1"
												</span>{" "}
												<span className={"text-green-400"}>200</span> 34
											</p>
											<p>
												<span
													className={"text-zinc-500"}
												>{`{"ts":"2026-03-05","lvl":"ERROR","svc":"api-gw","msg":"upstream timeout","latency":5001}`}</span>
											</p>
											<p>
												<span className={"text-zinc-600"}>Mar 5 14:22:33</span>{" "}
												prod-node-1 sshd: Failed password for root from
												203.0.113.1
											</p>
											<p>
												<span className={"text-zinc-600"}>[14:22:33]</span>{" "}
												<span className={"text-yellow-400"}>WARN</span>{" "}
												rate-limiter burst exceeded ip=10.0.0.8 limit=100/s
											</p>
											<p>
												<span className={"text-zinc-600"}>172.16.0.3</span> -
												bob{" "}
												<span className={"text-zinc-400"}>
													"DELETE /api/session HTTP/1.1"
												</span>{" "}
												<span className={"text-amber-400"}>403</span> 89
											</p>
										</div>
									))}
								</div>
								<div
									className={
										"absolute bottom-0 inset-x-0 h-20 bg-linear-to-t from-zinc-950 to-transparent pointer-events-none"
									}
								/>
							</div>
						</div>

						{/* Parsed output pane */}
						<div>
							<div
								className={
									"flex items-center justify-between gap-2 px-4 py-3 border-b border-zinc-800 bg-zinc-900/60"
								}
							>
								<div className={"flex items-center gap-2"}>
									<div className={"flex gap-1.5"}>
										<div className={"size-3 rounded-full bg-zinc-700"} />
										<div className={"size-3 rounded-full bg-zinc-700"} />
										<div className={"size-3 rounded-full bg-zinc-700"} />
									</div>
									<span className={"text-xs text-zinc-500 ml-2 font-mono"}>
										parsed output
									</span>
								</div>
								<span
									className={
										"flex items-center gap-1.5 text-[10px] font-mono text-green-400"
									}
								>
									<span
										className={
											"size-1.5 rounded-full bg-green-400 inline-block animate-[pulse_2s_ease-in-out_infinite]"
										}
									/>
									live
								</span>
							</div>

							<div
								className={
									"p-5 h-64 overflow-hidden font-mono text-[11px] leading-[1.8]"
								}
							>
								<p
									className={"text-zinc-600 mb-1"}
								>{`// 1,042 rows — 9 fields extracted`}</p>
								<p>
									<span className={"text-violet-400"}>timestamp</span>{" "}
									<span className={"text-zinc-600"}>→</span>{" "}
									<span className={"text-amber-300"}>
										"2026-03-05T14:22:31Z"
									</span>
								</p>
								<p>
									<span className={"text-violet-400"}>source_ip</span>
									{"  "}
									<span className={"text-zinc-600"}>→</span>{" "}
									<span className={"text-amber-300"}>"192.168.1.2"</span>
								</p>
								<p>
									<span className={"text-violet-400"}>user</span>
									{"       "}
									<span className={"text-zinc-600"}>→</span>{" "}
									<span className={"text-blue-300"}>"alice"</span>
								</p>
								<p>
									<span className={"text-violet-400"}>method</span>
									{"     "}
									<span className={"text-zinc-600"}>→</span>{" "}
									<span className={"text-blue-300"}>"POST"</span>
								</p>
								<p>
									<span className={"text-violet-400"}>path</span>
									{"       "}
									<span className={"text-zinc-600"}>→</span>{" "}
									<span className={"text-blue-300"}>"/ api/login"</span>
								</p>
								<p>
									<span className={"text-violet-400"}>status</span>
									{"     "}
									<span className={"text-zinc-600"}>→</span>{" "}
									<span className={"text-amber-400"}>401</span>
								</p>
								<p>
									<span className={"text-violet-400"}>bytes</span>
									{"      "}
									<span className={"text-zinc-600"}>→</span>{" "}
									<span className={"text-green-400"}>1247</span>
								</p>
								<p>
									<span className={"text-violet-400"}>level</span>
									{"      "}
									<span className={"text-zinc-600"}>→</span>{" "}
									<span className={"text-red-400"}>"CRITICAL"</span>
								</p>
								<p>
									<span className={"text-violet-400"}>format</span>
									{"     "}
									<span className={"text-zinc-600"}>→</span>{" "}
									<span className={"text-zinc-400"}>
										"combined+json+syslog"
									</span>
								</p>
							</div>
						</div>
					</div>
				</div>
			</section>

			{/* ─── Trusted by formats ──────────────────────────── */}
			<div className={"relative z-10 max-w-7xl mx-auto px-6"}>
				<Separator className={"bg-zinc-800/80"} />
			</div>

			{/* ─── Format Showcase ─────────────────────────────────────── */}
			<section
				id={"formats"}
				className={"relative z-10 px-6 py-24 max-w-7xl mx-auto"}
			>
				<div className={"text-center mb-14"}>
					<p
						className={
							"text-xs font-mono text-amber-400 tracking-[0.2em] uppercase mb-3"
						}
					>
						Universal compatibility
					</p>
					<h2 className={"text-4xl md:text-5xl font-bold tracking-tight mb-4"}>
						Every format. One parser.
					</h2>
					<p className={"text-zinc-400 text-lg max-w-xl mx-auto"}>
						Whether your logs come from a structured pipeline or were written
						freehand by a shell script from 2003 — Logdog handles it.
					</p>
				</div>

				<Tabs defaultValue={"structured"} className={"w-full"}>
					<TabsList
						className={cn(
							"flex justify-center w-fit mx-auto mb-10",
							"bg-zinc-900 border border-zinc-800 rounded-lg p-1 h-auto gap-1",
						)}
					>
						<TabsTrigger
							value={"structured"}
							className={cn(
								"rounded-md px-5 py-2 text-sm font-medium text-zinc-400 transition-all",
								"data-[state=active]:bg-amber-400 data-[state=active]:text-zinc-900 data-[state=active]:font-semibold data-[state=active]:shadow-none",
								"hover:text-zinc-200",
							)}
						>
							<Braces className={"size-3.5"} />
							Structured
						</TabsTrigger>
						<TabsTrigger
							value={"semi"}
							className={cn(
								"rounded-md px-5 py-2 text-sm font-medium text-zinc-400 transition-all",
								"data-[state=active]:bg-amber-400 data-[state=active]:text-zinc-900 data-[state=active]:font-semibold data-[state=active]:shadow-none",
								"hover:text-zinc-200",
							)}
						>
							<FileCode className={"size-3.5"} />
							Semi-structured
						</TabsTrigger>
						<TabsTrigger
							value={"unstructured"}
							className={cn(
								"rounded-md px-5 py-2 text-sm font-medium text-zinc-400 transition-all",
								"data-[state=active]:bg-amber-400 data-[state=active]:text-zinc-900 data-[state=active]:font-semibold data-[state=active]:shadow-none",
								"hover:text-zinc-200",
							)}
						>
							<FileText className={"size-3.5"} />
							Unstructured
						</TabsTrigger>
					</TabsList>

					{/* Structured */}
					<TabsContent value={"structured"}>
						<div className={"grid md:grid-cols-2 gap-10 items-start"}>
							<div>
								<p
									className={
										"text-xs font-mono text-amber-400 tracking-widest uppercase mb-4"
									}
								>
									JSON / NDJSON / CSV / Logfmt
								</p>
								<h3 className={"text-2xl font-bold mb-3"}>
									Schema-aligned from the start
								</h3>
								<p className={"text-zinc-400 mb-7 leading-relaxed"}>
									Structured logs already carry their schema. Logdog reads it,
									reconciles mixed formats, and loads everything into a coherent
									unified table — even when fields drift across versions.
								</p>
								<ul className={"space-y-2.5"}>
									{STRUCTURED_FORMATS.map((format) => (
										<li
											key={format}
											className={
												"flex items-center gap-2.5 text-sm text-zinc-300"
											}
										>
											<CheckCircle2
												className={"size-4 text-amber-400 shrink-0"}
											/>
											{format}
										</li>
									))}
								</ul>
							</div>

							<div
								className={
									"rounded-xl overflow-hidden border border-zinc-800 bg-zinc-900/50 shadow-xl"
								}
							>
								<div
									className={
										"px-4 py-3 border-b border-zinc-800 flex items-center gap-2 bg-zinc-900/80"
									}
								>
									<div className={"flex gap-1.5"}>
										<div className={"size-2.5 rounded-full bg-zinc-700"} />
										<div className={"size-2.5 rounded-full bg-zinc-700"} />
										<div className={"size-2.5 rounded-full bg-zinc-700"} />
									</div>
									<span className={"text-xs text-zinc-500 font-mono ml-1"}>
										app.log (NDJSON)
									</span>
								</div>
								<pre
									className={
										"px-5 py-5 text-[11px] font-mono text-zinc-300 overflow-x-auto leading-[1.8] whitespace-pre-wrap"
									}
								>
									{`{"ts":"2026-03-05T14:22:31Z","lvl":"ERROR",\n "svc":"auth","msg":"Token expired",\n "uid":"usr_9f2a","dur_ms":142}\n{"ts":"2026-03-05T14:22:32Z","lvl":"INFO",\n "svc":"api","msg":"Request completed",\n "path":"/users","status":200,"dur_ms":23}\n{"ts":"2026-03-05T14:22:33Z","lvl":"WARN",\n "svc":"db","msg":"Slow query detected",\n "table":"sessions","dur_ms":812}`}
								</pre>
							</div>
						</div>
					</TabsContent>

					{/* Semi-structured */}
					<TabsContent value={"semi"}>
						<div className={"grid md:grid-cols-2 gap-10 items-start"}>
							<div>
								<p
									className={
										"text-xs font-mono text-amber-400 tracking-widest uppercase mb-4"
									}
								>
									Apache / Nginx / Syslog / W3C
								</p>
								<h3 className={"text-2xl font-bold mb-3"}>
									Pattern-extracted, fully structured
								</h3>
								<p className={"text-zinc-400 mb-7 leading-relaxed"}>
									Semi-structured logs follow known conventions but aren't
									machine-readable out of the box. Logdog identifies the format,
									extracts all fields, and normalizes them automatically.
								</p>
								<ul className={"space-y-2.5"}>
									{SEMI_FORMATS.map((format) => (
										<li
											key={format}
											className={
												"flex items-center gap-2.5 text-sm text-zinc-300"
											}
										>
											<CheckCircle2
												className={"size-4 text-amber-400 shrink-0"}
											/>
											{format}
										</li>
									))}
								</ul>
							</div>

							<div
								className={
									"rounded-xl overflow-hidden border border-zinc-800 bg-zinc-900/50 shadow-xl"
								}
							>
								<div
									className={
										"px-4 py-3 border-b border-zinc-800 flex items-center gap-2 bg-zinc-900/80"
									}
								>
									<div className={"flex gap-1.5"}>
										<div className={"size-2.5 rounded-full bg-zinc-700"} />
										<div className={"size-2.5 rounded-full bg-zinc-700"} />
										<div className={"size-2.5 rounded-full bg-zinc-700"} />
									</div>
									<span className={"text-xs text-zinc-500 font-mono ml-1"}>
										nginx/access.log
									</span>
								</div>
								<pre
									className={
										"px-5 py-5 text-[11px] font-mono text-zinc-300 overflow-x-auto leading-[1.8] whitespace-pre-wrap"
									}
								>
									{`192.168.1.105 - alice [05/Mar/2026:14:22:31 +0000]\n"POST /api/v2/sessions HTTP/2.0" 201 512\n"https://app.example.com/login" "Chrome/121"\n\n10.0.0.2 - - [05/Mar/2026:14:22:32 +0000]\n"GET /health HTTP/1.1" 200 34\n"-" "kube-probe/1.28"\n\n203.0.113.1 - - [05/Mar/2026:14:22:33 +0000]\n"POST /admin/config HTTP/1.1" 403 89\n"-" "python-requests/2.28.0"`}
								</pre>
							</div>
						</div>
					</TabsContent>

					{/* Unstructured */}
					<TabsContent value={"unstructured"}>
						<div className={"grid md:grid-cols-2 gap-10 items-start"}>
							<div>
								<p
									className={
										"text-xs font-mono text-amber-400 tracking-widest uppercase mb-4"
									}
								>
									Plain text / Application output
								</p>
								<h3 className={"text-2xl font-bold mb-3"}>
									Raw text, made queryable
								</h3>
								<p className={"text-zinc-400 mb-7 leading-relaxed"}>
									No format at all? Logdog uses LLM-based extraction to find
									timestamps, severity, services, messages, and values — even in
									completely free-form text with no structure whatsoever.
								</p>
								<ul className={"space-y-2.5"}>
									{UNSTRUCTURED_FORMATS.map((format) => (
										<li
											key={format}
											className={
												"flex items-center gap-2.5 text-sm text-zinc-300"
											}
										>
											<CheckCircle2
												className={"size-4 text-amber-400 shrink-0"}
											/>
											{format}
										</li>
									))}
								</ul>
							</div>

							<div
								className={
									"rounded-xl overflow-hidden border border-zinc-800 bg-zinc-900/50 shadow-xl"
								}
							>
								<div
									className={
										"px-4 py-3 border-b border-zinc-800 flex items-center gap-2 bg-zinc-900/80"
									}
								>
									<div className={"flex gap-1.5"}>
										<div className={"size-2.5 rounded-full bg-zinc-700"} />
										<div className={"size-2.5 rounded-full bg-zinc-700"} />
										<div className={"size-2.5 rounded-full bg-zinc-700"} />
									</div>
									<span className={"text-xs text-zinc-500 font-mono ml-1"}>
										crash_dump.txt
									</span>
								</div>
								<pre
									className={
										"px-5 py-5 text-[11px] font-mono text-zinc-300 overflow-x-auto leading-[1.8] whitespace-pre-wrap"
									}
								>
									{`CRITICAL auth-worker crashed while processing\nrequest from prod-node-3 at 14:22:31 today --\nmemory limit exceeded (512MB), process\nrestarted, all 47 open connections dropped,\njob queue flushed. Last successful request\nwas 142ms prior. Contact: ops@example.com`}
								</pre>
							</div>
						</div>
					</TabsContent>
				</Tabs>
			</section>

			{/* ─── How It Works ───────────────────────────────────────── */}
			<section
				id={"how-it-works"}
				className={"relative z-10 px-6 py-24"}
				style={{
					background:
						"linear-gradient(to bottom, transparent 0%, rgba(255,255,255,0.012) 50%, transparent 100%)",
				}}
			>
				<div className={"max-w-7xl mx-auto"}>
					<div className={"text-center mb-16"}>
						<p
							className={
								"text-xs font-mono text-amber-400 tracking-[0.2em] uppercase mb-3"
							}
						>
							Simple by design
						</p>
						<h2 className={"text-4xl md:text-5xl font-bold tracking-tight"}>
							From raw chaos to clean data
							<br />
							in three steps.
						</h2>
					</div>

					<div className={"grid md:grid-cols-3 gap-6"}>
						{STEPS.map(({ number, title, description, detail }, index) => (
							<div key={index} className={"relative"}>
								{index < STEPS.length - 1 && (
									<div
										className={
											"hidden md:flex absolute top-10 left-[calc(100%+0.75rem)] w-6 items-center justify-center z-10"
										}
									>
										<ChevronRight className={"size-4 text-zinc-700"} />
									</div>
								)}
								<div
									className={
										"relative h-full p-8 rounded-2xl border border-zinc-800 bg-zinc-900/40 hover:border-zinc-700 hover:bg-zinc-900/70 transition-all duration-200"
									}
								>
									<div
										className={
											"text-6xl font-black text-amber-400/15 font-mono mb-5 leading-none select-none"
										}
									>
										{number}
									</div>
									<h3 className={"text-xl font-semibold mb-3"}>{title}</h3>
									<p className={"text-zinc-400 mb-5 text-sm leading-relaxed"}>
										{description}
									</p>
									<div
										className={
											"text-xs font-mono text-zinc-600 border-t border-zinc-800 pt-4"
										}
									>
										{detail}
									</div>
								</div>
							</div>
						))}
					</div>
				</div>
			</section>

			{/* ─── Features Grid ───────────────────────────────────────── */}
			<section
				id={"features"}
				className={"relative z-10 px-6 py-24 max-w-7xl mx-auto"}
			>
				<div className={"text-center mb-16"}>
					<p
						className={
							"text-xs font-mono text-amber-400 tracking-[0.2em] uppercase mb-3"
						}
					>
						What you get
					</p>
					<h2 className={"text-4xl md:text-5xl font-bold tracking-tight mb-4"}>
						The whole pipeline, built in.
					</h2>
					<p className={"text-zinc-400 text-lg max-w-xl mx-auto"}>
						Logdog isn't just a parser. It's the entire ingestion-to-insight
						stack, ready to use from your first upload.
					</p>
				</div>

				<div className={"grid sm:grid-cols-2 lg:grid-cols-3 gap-5"}>
					{FEATURES.map(({ Icon, title, description }, index) => (
						<Card
							key={index}
							className={
								"bg-zinc-900/40 border-zinc-800 hover:border-zinc-700 hover:bg-zinc-900/70 transition-all duration-200 gap-4 group"
							}
						>
							<CardHeader className={"pb-0"}>
								<div
									className={
										"size-10 rounded-lg bg-amber-400/10 border border-amber-400/20 flex items-center justify-center mb-1 group-hover:bg-amber-400/15 transition-colors"
									}
								>
									<Icon className={"size-5 text-amber-400"} />
								</div>
								<CardTitle className={"text-white text-base font-semibold"}>
									{title}
								</CardTitle>
							</CardHeader>
							<CardContent>
								<CardDescription
									className={"text-zinc-400 leading-relaxed text-sm"}
								>
									{description}
								</CardDescription>
							</CardContent>
						</Card>
					))}
				</div>
			</section>

			{/* ─── Stats ───────────────────────────────────────────────── */}
			<section className={"relative z-10 px-6 py-6 max-w-4xl mx-auto"}>
				<div
					className={
						"grid grid-cols-3 divide-x divide-zinc-800 border border-zinc-800 rounded-2xl overflow-hidden"
					}
					style={{ background: "rgba(255,255,255,0.015)" }}
				>
					{STATS.map(({ value, label }, index) => (
						<div key={index} className={"p-10 text-center"}>
							<div
								className={"text-5xl font-black text-amber-400 mb-2 font-mono"}
							>
								{value}
							</div>
							<div className={"text-sm text-zinc-400"}>{label}</div>
						</div>
					))}
				</div>
			</section>

			{/* ─── CTA ─────────────────────────────────────────────────── */}
			<section
				className={"relative z-10 px-6 py-32 text-center overflow-hidden"}
			>
				<div
					className={"absolute inset-0 pointer-events-none"}
					style={{
						background:
							"radial-gradient(ellipse 70% 70% at 50% 50%, rgba(245,158,11,0.055) 0%, transparent 65%)",
					}}
				/>
				<div className={"relative"}>
					<p
						className={
							"text-xs font-mono text-amber-400 tracking-[0.2em] uppercase mb-5"
						}
					>
						Get started free
					</p>
					<h2
						className={
							"text-4xl md:text-5xl lg:text-6xl font-extrabold tracking-tight mb-6 max-w-3xl mx-auto leading-[1.08]"
						}
					>
						Your logs have been trying to tell you something.
					</h2>
					<p className={"text-zinc-400 text-lg mb-10 max-w-md mx-auto"}>
						Start parsing in minutes. No card required. No configuration needed.
						Just upload and go.
					</p>
					<Button
						size={"lg"}
						className={
							"bg-amber-400 text-zinc-900 hover:bg-amber-300 font-semibold h-12 px-8 text-base gap-2 shadow-none"
						}
						asChild
					>
						<Link href={"/auth/new"}>
							Start parsing free
							<ArrowRight className={"size-4"} />
						</Link>
					</Button>
				</div>
			</section>

			{/* ─── Footer ──────────────────────────────────────────────── */}
			<footer
				className={"relative z-10 border-t border-zinc-800/80 px-6 py-10"}
			>
				<div
					className={
						"max-w-7xl mx-auto flex flex-col md:flex-row items-center justify-between gap-5"
					}
				>
					<div className={"flex items-center gap-2.5"}>
						<div
							className={
								"size-7 rounded-lg bg-amber-400 flex items-center justify-center shrink-0"
							}
						>
							<Terminal className={"size-3.5 text-zinc-900"} />
						</div>
						<span className={"font-bold text-sm"}>logdog</span>
					</div>

					<p className={"text-xs text-zinc-600"}>
						© 2026 Logdog. All logs welcome.
					</p>

					<div className={"flex gap-7 text-xs text-zinc-500"}>
						<Link
							href={"#"}
							className={"hover:text-zinc-300 transition-colors"}
						>
							Privacy
						</Link>
						<Link
							href={"#"}
							className={"hover:text-zinc-300 transition-colors"}
						>
							Terms
						</Link>
						<Link
							href={"#"}
							className={"hover:text-zinc-300 transition-colors"}
						>
							Docs
						</Link>
					</div>
				</div>
			</footer>
		</div>
	);
}

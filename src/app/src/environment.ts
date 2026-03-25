const LOCAL_API_HOSTS = new Set(["localhost", "127.0.0.1", "::1", "[::1]"]);

export const isDevelopment = process.env.NODE_ENV !== "production";
export const isProduction = process.env.NODE_ENV === "production";

function normalizeApiUrl(value: string | undefined) {
	const trimmedValue = (value ?? "").trim();
	if (trimmedValue.length === 0) {
		throw new Error("Missing NEXT_PUBLIC_API_URL environment variable.");
	}

	let parsedUrl: URL;
	try {
		parsedUrl = new URL(trimmedValue);
	} catch {
		throw new Error("Invalid NEXT_PUBLIC_API_URL environment variable.");
	}

	const protocol = parsedUrl.protocol.toLowerCase();
	if (protocol !== "http:" && protocol !== "https:") {
		throw new Error("NEXT_PUBLIC_API_URL must use http or https.");
	}

	const hostname = parsedUrl.hostname.toLowerCase();
	const isLocalHost = LOCAL_API_HOSTS.has(hostname);
	const isHttpsPage = typeof window !== "undefined" && window.location.protocol === "https:";

	// Prevent mixed-content calls in production/cross-site HTTPS pages.
	if (protocol === "http:" && !isLocalHost && (isHttpsPage || isProduction)) {
		parsedUrl.protocol = "https:";
	}

	// Keep baseURL stable regardless of whether env value ends with '/'.
	const normalizedPath = parsedUrl.pathname.replace(/\/+$/, "");
	return `${parsedUrl.origin}${normalizedPath}`;
}

export const API_URL = normalizeApiUrl(process.env.NEXT_PUBLIC_API_URL);

const LOCAL_API_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);

function normalizeApiUrl(value: string) {
	const trimmedValue = value.trim();
	if (trimmedValue.length === 0) {
		throw new Error("Missing NEXT_PUBLIC_API_URL environment variable.");
	}

	// Trim trailing slashes so path joins stay predictable.
	let normalizedValue = trimmedValue.replace(/\/+$/, "");

	// Guard production pages against mixed-content API URLs.
	if (
		typeof window !== "undefined" &&
		window.location.protocol === "https:" &&
		normalizedValue.startsWith("http://")
	) {
		const host =
			normalizedValue.slice("http://".length).split("/")[0]?.toLowerCase() ??
			"";
		if (!LOCAL_API_HOSTS.has(host)) {
			normalizedValue = `https://${normalizedValue.slice("http://".length)}`;
		}
	}

	return normalizedValue;
}

export const API_URL = normalizeApiUrl(process.env.NEXT_PUBLIC_API_URL ?? "");

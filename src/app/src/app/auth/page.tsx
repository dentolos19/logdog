"use client";

import { zodResolver } from "@hookform/resolvers/zod";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useState } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";

import { useAuth } from "@/components/auth-provider";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/spinner";

const schema = z.object({
	email: z.email("Please enter a valid email address."),
	password: z.string().min(1, "Password is required."),
});

type FormValues = z.infer<typeof schema>;

export default function LoginPage() {
	const { signIn } = useAuth();
	const router = useRouter();
	const [serverError, setServerError] = useState<string | null>(null);

	const form = useForm<FormValues>({
		resolver: zodResolver(schema),
		defaultValues: { email: "", password: "" },
	});

	const onSubmit = async (values: FormValues) => {
		setServerError(null);
		try {
			await signIn(values.email, values.password);
			router.push("/dashboard");
		} catch (err) {
			setServerError(err instanceof Error ? err.message : "Something went wrong.");
		}
	};

	return (
		<div className={"flex min-h-screen items-center justify-center p-4"}>
			<Card className={"w-full max-w-sm"}>
				<CardHeader>
					<CardTitle className={"text-xl"}>{"Sign in"}</CardTitle>
					<CardDescription>{"Enter your credentials to access your account."}</CardDescription>
				</CardHeader>

				<CardContent>
					<Form {...form}>
						<form className={"flex flex-col gap-4"} onSubmit={form.handleSubmit(onSubmit)}>
							<FormField
								control={form.control}
								name={"email"}
								render={({ field }) => (
									<FormItem>
										<FormLabel>{"Email"}</FormLabel>
										<FormControl>
											<Input autoComplete={"email"} placeholder={"you@example.com"} type={"email"} {...field} />
										</FormControl>
										<FormMessage />
									</FormItem>
								)}
							/>

							<FormField
								control={form.control}
								name={"password"}
								render={({ field }) => (
									<FormItem>
										<FormLabel>{"Password"}</FormLabel>
										<FormControl>
											<Input autoComplete={"current-password"} type={"password"} {...field} />
										</FormControl>
										<FormMessage />
									</FormItem>
								)}
							/>

							{serverError && <p className={"text-destructive text-sm"}>{serverError}</p>}

							<Button className={"w-full"} disabled={form.formState.isSubmitting} type={"submit"}>
								{form.formState.isSubmitting ? <Spinner /> : "Sign in"}
							</Button>
						</form>
					</Form>
				</CardContent>

				<CardFooter className={"justify-center text-muted-foreground text-sm"}>
					{"Don't have an account?\u00a0"}
					<Link className={"text-foreground underline-offset-4 hover:underline"} href={"/auth/new"}>
						{"Sign up"}
					</Link>
				</CardFooter>
			</Card>
		</div>
	);
}

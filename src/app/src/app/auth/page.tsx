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
            <form onSubmit={form.handleSubmit(onSubmit)} className={"flex flex-col gap-4"}>
              <FormField
                control={form.control}
                name={"email"}
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>{"Email"}</FormLabel>
                    <FormControl>
                      <Input type={"email"} placeholder={"you@example.com"} autoComplete={"email"} {...field} />
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
                      <Input type={"password"} autoComplete={"current-password"} {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              {serverError && <p className={"text-sm text-destructive"}>{serverError}</p>}

              <Button type={"submit"} className={"w-full"} disabled={form.formState.isSubmitting}>
                {form.formState.isSubmitting ? <Spinner /> : "Sign in"}
              </Button>
            </form>
          </Form>
        </CardContent>

        <CardFooter className={"justify-center text-sm text-muted-foreground"}>
          {"Don't have an account?\u00a0"}
          <Link href={"/auth/new"} className={"text-foreground underline-offset-4 hover:underline"}>
            {"Sign up"}
          </Link>
        </CardFooter>
      </Card>
    </div>
  );
}

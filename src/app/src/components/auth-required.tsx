import { Link } from "@tanstack/react-router";
import { Button } from "#/components/ui/button";
import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyTitle } from "#/components/ui/empty";

export default function AuthRequired() {
  return (
    <div className={"flex size-full items-center justify-center"}>
      <Empty>
        <EmptyHeader>
          <EmptyTitle>Access Denied</EmptyTitle>
          <EmptyDescription>You need to be signed in to access this resource.</EmptyDescription>
        </EmptyHeader>
        <EmptyContent>
          <Button asChild>
            <Link to={"/auth"}>Login</Link>
          </Button>
        </EmptyContent>
      </Empty>
    </div>
  );
}

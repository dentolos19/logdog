import { Link, useLocation, useNavigate } from "@tanstack/react-router";
import {
  ChevronUpIcon,
  FlaskConicalIcon,
  LayoutDashboardIcon,
  LogOutIcon,
  ScrollTextIcon,
  UserCircle2Icon,
} from "lucide-react";
import { useAuth } from "#/components/auth-provider";
import { Avatar, AvatarFallback } from "#/components/ui/avatar";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "#/components/ui/dropdown-menu";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "#/components/ui/sidebar";
import { isDevelopment } from "#/environment";

const baseNavigation = [
  { label: "Dashboard", href: "/dashboard", icon: LayoutDashboardIcon },
  { label: "Logs", href: "/logs", icon: ScrollTextIcon },
];

const devNavigation = [{ label: "Development", href: "/development", icon: FlaskConicalIcon }];
const navigation = isDevelopment ? [...baseNavigation, ...devNavigation] : baseNavigation;

export function AppSidebar() {
  const location = useLocation();
  const navigate = useNavigate();
  const { user, signOut } = useAuth();

  const avatarInitials = (user?.email ?? "?").slice(0, 2).toUpperCase();

  const onSignOut = async () => {
    await signOut();
    await navigate({ to: "/auth" });
  };

  return (
    <Sidebar>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              {navigation.map((item) => (
                <SidebarMenuItem key={item.href}>
                  <SidebarMenuButton asChild isActive={location.pathname === item.href}>
                    <Link to={item.href}>
                      <item.icon />
                      <span>{item.label}</span>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter>
        <SidebarMenu>
          <SidebarMenuItem>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <SidebarMenuButton size={"lg"}>
                  <Avatar size={"sm"}>
                    <AvatarFallback>{avatarInitials}</AvatarFallback>
                  </Avatar>
                  <span className={"flex-1 truncate text-sm"}>{user?.email}</span>
                  <ChevronUpIcon className={"ml-auto"} />
                </SidebarMenuButton>
              </DropdownMenuTrigger>

              <DropdownMenuContent align={"start"} side={"top"}>
                <DropdownMenuLabel className={"truncate"}>{user?.email}</DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuItem>
                  <UserCircle2Icon />
                  Signed in
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => void onSignOut()} variant={"destructive"}>
                  <LogOutIcon />
                  Sign out
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
    </Sidebar>
  );
}
